/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "service/raft/raft_sys_table_storage.hh"

#include "cql3/untyped_result_set.hh"
#include "db/system_keyspace.hh"
#include "utils/UUID.hh"

#include "serializer.hh"
#include "idl/raft_storage.dist.hh"
#include "serializer_impl.hh"
#include "idl/raft_storage.dist.impl.hh"

#include "cql3/statements/batch_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "cql3/query_processor.hh"

#include "gms/inet_address_serializer.hh"

#include <seastar/core/loop.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

namespace service {

raft_sys_table_storage::raft_sys_table_storage(cql3::query_processor& qp, raft::group_id gid, raft::server_id server_id)
    : _group_id(std::move(gid))
    , _server_id(std::move(server_id))
    , _qp(qp)
    , _dummy_query_state(service::client_state::for_internal_calls(), empty_service_permit())
    , _pending_op_fut(make_ready_future<>())
{
    static const auto store_cql = format("INSERT INTO system.{} (group_id, term, \"index\", data) VALUES (?, ?, ?, ?)",
        db::system_keyspace::RAFT);
    auto prepared_stmt_ptr = _qp.prepare_internal(store_cql);
    shared_ptr<cql3::cql_statement> cql_stmt = prepared_stmt_ptr->statement;
    _store_entry_stmt = dynamic_pointer_cast<cql3::statements::modification_statement>(cql_stmt);
}

future<> raft_sys_table_storage::store_term_and_vote(raft::term_t term, raft::server_id vote) {
    return execute_with_linearization_point([this, term, vote] {
        static const auto store_cql = format("INSERT INTO system.{} (group_id, vote_term, vote) VALUES (?, ?, ?)",
            db::system_keyspace::RAFT);
        return _qp.execute_internal(
            store_cql,
            {_group_id.id, int64_t(term), vote.id}).discard_result();
    });
}

future<std::pair<raft::term_t, raft::server_id>> raft_sys_table_storage::load_term_and_vote() {
    static const auto load_cql = format("SELECT vote_term, vote FROM system.{} WHERE group_id = ? LIMIT 1", db::system_keyspace::RAFT);
    ::shared_ptr<cql3::untyped_result_set> rs = co_await _qp.execute_internal(load_cql, {_group_id.id});
    if (rs->empty()) {
        co_return std::pair(raft::term_t(), raft::server_id());
    }
    const auto& static_row = rs->one();
    raft::term_t vote_term = raft::term_t(static_row.get_or<int64_t>("vote_term", raft::term_t{}));
    raft::server_id vote{static_row.get_or<utils::UUID>("vote", raft::server_id{}.id)};
    co_return std::pair(vote_term, vote);
}

future<> raft_sys_table_storage::store_commit_idx(raft::index_t idx) {
    return execute_with_linearization_point([this, idx] {
        static const auto store_cql = format("INSERT INTO system.{} (group_id, commit_idx) VALUES (?, ?)",
            db::system_keyspace::RAFT);
        return _qp.execute_internal(
            store_cql,
            {_group_id.id, int64_t(idx)}).discard_result();
    });
}

future<raft::index_t> raft_sys_table_storage::load_commit_idx() {
    static const auto load_cql = format("SELECT commit_idx FROM system.{} WHERE group_id = ? LIMIT 1", db::system_keyspace::RAFT);
    ::shared_ptr<cql3::untyped_result_set> rs = co_await _qp.execute_internal(load_cql, {_group_id.id});
    if (rs->empty()) {
        co_return raft::index_t(0);
    }
    const auto& static_row = rs->one();
    co_return raft::index_t(static_row.get_or<int64_t>("commit_idx", raft::index_t{}));
}


future<raft::log_entries> raft_sys_table_storage::load_log() {
    static const auto load_cql = format("SELECT term, \"index\", data FROM system.{} WHERE group_id = ?", db::system_keyspace::RAFT);
    ::shared_ptr<cql3::untyped_result_set> rs = co_await _qp.execute_internal(load_cql, {_group_id.id});

    raft::log_entries log;
    for (const cql3::untyped_result_set_row& row : *rs) {
        if (!row.has("data")) {
            // The partition only contains static cells, the log
            // is empty.
            break;
        }
        raft::term_t term = raft::term_t(row.get_as<int64_t>("term"));
        raft::index_t idx = raft::index_t(row.get_as<int64_t>("index"));
        auto raw_data = row.get_blob("data");
        auto in = ser::as_input_stream(raw_data);
        using data_variant_type = decltype(raft::log_entry::data);
        data_variant_type data = ser::deserialize(in, boost::type<data_variant_type>());

        log.emplace_back(make_lw_shared<const raft::log_entry>(
            raft::log_entry{.term = term, .idx = idx, .data = std::move(data)}));

        co_await coroutine::maybe_yield();
    }
    co_return log;
}

future<raft::snapshot_descriptor> raft_sys_table_storage::load_snapshot_descriptor() {
    static const auto load_id_cql = format("SELECT snapshot_id FROM system.{} WHERE group_id = ? LIMIT 1", db::system_keyspace::RAFT);
    ::shared_ptr<cql3::untyped_result_set> id_rs = co_await _qp.execute_internal(load_id_cql, {_group_id.id});
    if (id_rs->empty() || !id_rs->one().has("snapshot_id")) {
        co_return raft::snapshot_descriptor();
    }
    const auto& id_row = id_rs->one(); // should be only one row since snapshot_id column is static
    utils::UUID snapshot_id = id_row.get_as<utils::UUID>("snapshot_id");

    // Fetch raft log index and term for the latest snapshot descriptor
    static const auto load_snp_info_cql = format("SELECT idx, term FROM system.{} WHERE group_id = ? AND server_id = ?",
        db::system_keyspace::RAFT_SNAPSHOTS);
    ::shared_ptr<cql3::untyped_result_set> snp_rs = co_await _qp.execute_internal(load_snp_info_cql, {_group_id.id, _server_id.id});
    // Should be only one matching row, since each individual server can only
    // have a single snapshot installed at a time
    const auto& snp_row = snp_rs->one();
    // Fetch current and previous raft configurations for the snapshot
    static const auto  load_cfg_cql = format("SELECT server_id, disposition, can_vote, ip_addr FROM system.{} WHERE group_id = ? AND my_server_id = ?", db::system_keyspace::RAFT_CONFIG);
    ::shared_ptr<cql3::untyped_result_set> cfg_rs = co_await _qp.execute_internal(load_cfg_cql, {_group_id.id, _server_id.id});

    raft::configuration cfg;

    for (const cql3::untyped_result_set_row& row : *cfg_rs) {
        const auto disposition = row.get_as<sstring>("disposition");
        auto& cfg_part = disposition == "CURRENT" ? cfg.current : cfg.previous;
        cfg_part.insert(raft::server_address{
            raft::server_id{row.get_as<utils::UUID>("server_id")},
            row.get_as<bool>("can_vote"),
            ser::serialize_to_buffer<bytes>(gms::inet_address{row.get_as<net::inet_address>("ip_addr")})});
    }

    raft::snapshot_descriptor s{
        .idx = raft::index_t(snp_row.get_as<int64_t>("idx")),
        .term = raft::term_t(snp_row.get_as<int64_t>("term")),
        .config = std::move(cfg),
        .id = raft::snapshot_id(snapshot_id)};
    co_return s;
}

future<> raft_sys_table_storage::store_snapshot_descriptor(const raft::snapshot_descriptor& snap, size_t preserve_log_entries) {
    // TODO: check that snap.idx refers to an already persisted entry
    return execute_with_linearization_point([this, &snap, preserve_log_entries] () -> future<> {
        static const auto store_snp_cql = format("INSERT INTO system.{} (group_id, server_id, snapshot_id, idx, term) VALUES (?, ?, ?, ?, ?)",
            db::system_keyspace::RAFT_SNAPSHOTS);
        co_await _qp.execute_internal(
            store_snp_cql,
            {_group_id.id, _server_id.id, snap.id.id, int64_t(snap.idx), int64_t(snap.term)}
        );
        // remove old configs
        static const auto delete_raft_cfg_cql = format("DELETE FROM system.{} WHERE group_id = ? AND my_server_id = ?", db::system_keyspace::RAFT_CONFIG);
        co_await _qp.execute_internal(delete_raft_cfg_cql, {_group_id.id, _server_id.id});
        // store current and previous raft configurations
        static const auto store_raft_cfg_cql = format("INSERT INTO system.{} (group_id, my_server_id, server_id, disposition, can_vote, ip_addr) VALUES (?, ?, ?, ?, ?, ?)",
            db::system_keyspace::RAFT_CONFIG);
        for (const raft::server_address& srv_addr : snap.config.current) {
            co_await _qp.execute_internal(store_raft_cfg_cql,
                {_group_id.id, _server_id.id, srv_addr.id.id, "CURRENT", srv_addr.can_vote,
                    ser::deserialize_from_buffer(srv_addr.info, boost::type<gms::inet_address>()).addr()});
        }
        for (const raft::server_address& srv_addr : snap.config.previous) {
            co_await _qp.execute_internal(store_raft_cfg_cql,
                {_group_id.id, _server_id.id, srv_addr.id.id, "PREVIOUS", srv_addr.can_vote,
                    ser::deserialize_from_buffer(srv_addr.info, boost::type<gms::inet_address>()).addr()});
        }
        // Also update the latest snapshot id in `system.raft` table
        static const auto store_latest_id_cql = format("INSERT INTO system.{} (group_id, snapshot_id) VALUES (?, ?)",
            db::system_keyspace::RAFT);
        co_await _qp.execute_internal(
            store_latest_id_cql,
            {_group_id.id, snap.id.id}
        );
        if (preserve_log_entries > snap.idx) {
            co_return;
        }
        // TODO: make truncation and snapshot update in `system.raft` atomic
        co_await truncate_log_tail(raft::index_t(static_cast<uint64_t>(snap.idx) - static_cast<uint64_t>(preserve_log_entries)));
    });
}

future<> raft_sys_table_storage::do_store_log_entries(const std::vector<raft::log_entry_ptr>& entries) {
    if (entries.empty()) {
        co_return;
    }
    std::vector<cql3::statements::batch_statement::single_statement> batch_stmts;
    // statement values that can be allocated at once (one contiguous allocation)
    std::vector<std::vector<cql3::raw_value>> stmt_values;
    // fragmented storage for log_entries data
    std::vector<fragmented_temporary_buffer> stmt_data_views;
    // statement value views -- required for `query_options` to consume `fragmented_temporary_buffer::view`
    std::vector<std::vector<cql3::raw_value_view>> stmt_value_views;
    const size_t entries_size = entries.size();
    batch_stmts.reserve(entries_size);
    stmt_values.reserve(entries_size);
    stmt_data_views.reserve(entries_size);
    stmt_value_views.reserve(entries_size);

    for (const raft::log_entry_ptr& eptr : entries) {
        batch_stmts.emplace_back(cql3::statements::batch_statement::single_statement(_store_entry_stmt, false));

        auto data_tmp_buf = fragmented_temporary_buffer::allocate_to_fit(ser::get_sizeof(eptr->data));
        auto data_out_str = data_tmp_buf.get_ostream();
        ser::serialize(data_out_str, eptr->data);

        // don't include serialized "data" here since it will require to linearize the stream
        std::vector<cql3::raw_value> single_stmt_values;
        // Silly workaround for https://bugs.llvm.org/show_bug.cgi?id=51515
        single_stmt_values.reserve(3);
        single_stmt_values.emplace_back(cql3::raw_value::make_value(timeuuid_type->decompose(_group_id.id)));
        single_stmt_values.emplace_back(cql3::raw_value::make_value(long_type->decompose(int64_t(eptr->term))));
        single_stmt_values.emplace_back(cql3::raw_value::make_value(long_type->decompose(int64_t(eptr->idx))));

        stmt_values.emplace_back(std::move(single_stmt_values));
        stmt_data_views.emplace_back(std::move(data_tmp_buf));

        // allocate value views
        std::vector<cql3::raw_value_view> value_views;
        value_views.reserve(4); // 4 is the number of required values for the insertion query 
        for (const cql3::raw_value& raw : stmt_values.back()) {
            value_views.push_back(raw.to_view());
        }
        value_views.emplace_back(
            cql3::raw_value_view::make_value(
                fragmented_temporary_buffer::view(stmt_data_views.back())));
        stmt_value_views.emplace_back(std::move(value_views));

        co_await coroutine::maybe_yield();
    }

    auto batch_options = cql3::query_options::make_batch_options(
        cql3::query_options(
            cql3::default_cql_config,
            db::consistency_level::ONE,
            std::nullopt,
            std::vector<cql3::raw_value>{},
            false,
            cql3::query_options::specific_options::DEFAULT,
            cql_serialization_format::latest()),
        std::move(stmt_value_views));

    cql3::statements::batch_statement batch(
        cql3::statements::batch_statement::type::UNLOGGED,
        std::move(batch_stmts),
        cql3::attributes::none(),
        _qp.get_cql_stats());

    co_await batch.execute(_qp, _dummy_query_state, batch_options);
}

future<> raft_sys_table_storage::store_log_entries(const std::vector<raft::log_entry_ptr>& entries) {
    return execute_with_linearization_point([this, &entries] {
        return do_store_log_entries(entries);
    });
}

future<> raft_sys_table_storage::truncate_log(raft::index_t idx) {
    return execute_with_linearization_point([this, idx] {
        static const auto truncate_cql = format("DELETE FROM system.{} WHERE group_id = ? AND \"index\" >= ?",
            db::system_keyspace::RAFT); 
        return _qp.execute_internal(truncate_cql, {_group_id.id, int64_t(idx)}).discard_result();
    });
}

future<> raft_sys_table_storage::abort() {
    // wait for pending write requests to complete.
    // TODO: should we wait for all kinds of requests?
    return std::move(_pending_op_fut);
}

future<> raft_sys_table_storage::truncate_log_tail(raft::index_t idx) {
    static const auto truncate_cql = format("DELETE FROM system.{} WHERE group_id = ? AND \"index\" <= ?", db::system_keyspace::RAFT);
    return _qp.execute_internal(truncate_cql, {_group_id.id, int64_t(idx)}).discard_result();
}

future<> raft_sys_table_storage::execute_with_linearization_point(std::function<future<>()> f) {
    promise<> task_promise;
    auto pending_fut = std::exchange(_pending_op_fut, task_promise.get_future());
    co_await std::move(pending_fut);
    try {
        co_await f();
        task_promise.set_value();
    } catch (...) {
        task_promise.set_exception(std::current_exception());
        throw;
    }
}

future<> raft_sys_table_storage::bootstrap(raft::configuration initial_configuation) {
    raft::snapshot_descriptor snapshot;
    snapshot.config = std::move(initial_configuation);
    co_await store_snapshot_descriptor(snapshot, 0);
}

} // end of namespace service