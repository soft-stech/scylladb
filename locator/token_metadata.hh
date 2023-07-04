/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <map>
#include <unordered_set>
#include <unordered_map>
#include "gms/inet_address.hh"
#include "dht/i_partitioner.hh"
#include "locator/token_range_splitter.hh"
#include "inet_address_vectors.hh"
#include <optional>
#include <memory>
#include <boost/range/iterator_range.hpp>
#include <boost/icl/interval.hpp>
#include "range.hh"
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include "utils/phased_barrier.hh"
#include "service/topology_state_machine.hh"

#include "locator/types.hh"
#include "locator/topology.hh"

// forward declaration since replica/database.hh includes this file
namespace replica {
class keyspace;
}

namespace locator {

class abstract_replication_strategy;

using token = dht::token;

class token_metadata;
class tablet_metadata;

struct host_id_or_endpoint {
    host_id id;
    gms::inet_address endpoint;

    enum class param_type {
        host_id,
        endpoint,
        auto_detect
    };

    host_id_or_endpoint(const sstring& s, param_type restrict = param_type::auto_detect);

    bool has_host_id() const noexcept {
        return bool(id);
    }

    bool has_endpoint() const noexcept {
        return endpoint != gms::inet_address();
    }

    // Map the host_id to endpoint based on whichever of them is set,
    // using the token_metadata
    void resolve(const token_metadata& tm);
};

class token_metadata_impl;
struct topology_change_info;

class token_metadata final {
    std::unique_ptr<token_metadata_impl> _impl;
public:
    struct config {
        topology::config topo_cfg;
    };
    using inet_address = gms::inet_address;
    using version_t = service::topology::version_t;
    using version_tracker_t = utils::phased_barrier::operation;
private:
    friend class token_metadata_ring_splitter;
    class tokens_iterator {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = token;
        using difference_type = std::ptrdiff_t;
        using pointer = token*;
        using reference = token&;
    public:
        tokens_iterator() = default;
        tokens_iterator(const token& start, const token_metadata_impl* token_metadata);
        bool operator==(const tokens_iterator& it) const;
        const token& operator*() const;
        tokens_iterator& operator++();
    private:
        std::vector<token>::const_iterator _cur_it;
        size_t _remaining = 0;
        const token_metadata_impl* _token_metadata = nullptr;

        friend class token_metadata_impl;
    };

public:
    token_metadata(config cfg);
    explicit token_metadata(std::unique_ptr<token_metadata_impl> impl);
    token_metadata(token_metadata&&) noexcept; // Can't use "= default;" - hits some static_assert in unique_ptr
    token_metadata& operator=(token_metadata&&) noexcept;
    ~token_metadata();
    const std::vector<token>& sorted_tokens() const;
    const tablet_metadata& tablets() const;
    void set_tablets(tablet_metadata);
    // Update token->endpoint mappings for a given \c endpoint.
    // \c tokens are all the tokens that are now owned by \c endpoint.
    //
    // Note: the function is not exception safe!
    // It must be called only on a temporary copy of the token_metadata
    future<> update_normal_tokens(std::unordered_set<token> tokens, inet_address endpoint);
    const token& first_token(const token& start) const;
    size_t first_token_index(const token& start) const;
    std::optional<inet_address> get_endpoint(const token& token) const;
    std::vector<token> get_tokens(const inet_address& addr) const;
    const std::unordered_map<token, inet_address>& get_token_to_endpoint() const;
    const std::unordered_set<inet_address>& get_leaving_endpoints() const;
    const std::unordered_map<token, inet_address>& get_bootstrap_tokens() const;

    /**
     * Update or add endpoint given its inet_address and endpoint_dc_rack.
     */
    void update_topology(inet_address ep, endpoint_dc_rack dr, std::optional<node::state> opt_st = std::nullopt,
                         std::optional<shard_id> shard_count = std::nullopt);
    /**
     * Creates an iterable range of the sorted tokens starting at the token t
     * such that t >= start.
     *
     * @param start A token that will define the beginning of the range
     *
     * @return The requested range (see the description above)
     */
    boost::iterator_range<tokens_iterator> ring_range(const token& start) const;

    /**
     * Returns a range of tokens such that the first token t satisfies dht::ring_position_view::ending_at(t) >= start.
     */
    boost::iterator_range<tokens_iterator> ring_range(dht::ring_position_view start) const;

    topology& get_topology();
    const topology& get_topology() const;
    void debug_show() const;

    /**
     * Store an end-point to host ID mapping.  Each ID must be unique, and
     * cannot be changed after the fact.
     *
     * @param hostId
     * @param endpoint
     */
    void update_host_id(const locator::host_id& host_id, inet_address endpoint);

    /** Return the unique host ID for an end-point. */
    host_id get_host_id(inet_address endpoint) const;

    /// Return the unique host ID for an end-point or nullopt if not found.
    std::optional<host_id> get_host_id_if_known(inet_address endpoint) const;

    /** Return the end-point for a unique host ID */
    std::optional<inet_address> get_endpoint_for_host_id(locator::host_id host_id) const;

    /// Parses the \c host_id_string either as a host uuid or as an ip address and returns the mapping.
    /// Throws std::invalid_argument on parse error or std::runtime_error if the host_id wasn't found.
    host_id_or_endpoint parse_host_id_and_endpoint(const sstring& host_id_string) const;

    /** @return a copy of the endpoint-to-id map for read-only operations */
    std::unordered_map<inet_address, host_id> get_endpoint_to_host_id_map_for_reading() const;

    /// Returns host_id of the local node.
    host_id get_my_id() const;

    void add_bootstrap_token(token t, inet_address endpoint);

    void add_bootstrap_tokens(std::unordered_set<token> tokens, inet_address endpoint);

    void remove_bootstrap_tokens(std::unordered_set<token> tokens);

    void add_leaving_endpoint(inet_address endpoint);
    void del_leaving_endpoint(inet_address endpoint);

    void remove_endpoint(inet_address endpoint);

    // Checks if the node is part of the token ring. If yes, the node is one of
    // the nodes that owns the tokens and inside the set _normal_token_owners.
    bool is_normal_token_owner(inet_address endpoint) const;

    bool is_leaving(inet_address endpoint) const;

    // Is this node being replaced by another node
    bool is_being_replaced(inet_address endpoint) const;

    // Is any node being replaced by another node
    bool is_any_node_being_replaced() const;

    void add_replacing_endpoint(inet_address existing_node, inet_address replacing_node);

    void del_replacing_endpoint(inet_address existing_node);

    /**
     * Create a full copy of token_metadata using asynchronous continuations.
     * The caller must ensure that the cloned object will not change if
     * the function yields.
     */
    future<token_metadata> clone_async() const noexcept;

    /**
     * Create a copy of TokenMetadata with only tokenToEndpointMap. That is, pending ranges,
     * bootstrap tokens and leaving endpoints are not included in the copy.
     * The caller must ensure that the cloned object will not change if
     * the function yields.
     */
    future<token_metadata> clone_only_token_map() const noexcept;
    /**
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave operations have finished.
     * The caller must ensure that the cloned object will not change if
     * the function yields.
     *
     * @return a future holding a new token metadata
     */
    future<token_metadata> clone_after_all_left() const noexcept;

    /**
     * Gently clear the token_metadata members.
     * Yield if needed to prevent reactor stalls.
     */
    future<> clear_gently() noexcept;

    /*
     * Number of returned ranges = O(tokens.size())
     */
    dht::token_range_vector get_primary_ranges_for(std::unordered_set<token> tokens) const;

    /*
     * Number of returned ranges = O(1)
     */
    dht::token_range_vector get_primary_ranges_for(token right) const;
    static boost::icl::interval<token>::interval_type range_to_interval(range<dht::token> r);
    static range<dht::token> interval_to_range(boost::icl::interval<token>::interval_type i);

    future<> update_topology_change_info(dc_rack_fn& get_dc_rack);

    const std::optional<topology_change_info>& get_topology_change_info() const;

    token get_predecessor(token t) const;

    const std::unordered_set<inet_address>& get_all_endpoints() const;

    /* Returns the number of different endpoints that own tokens in the ring.
     * Bootstrapping tokens are not taken into account. */
    size_t count_normal_token_owners() const;

    // Updates the read_new flag, switching read requests from
    // the old endpoints to the new ones during topology changes:
    // read_new_t::no - no read_endpoints will be stored on update_pending_ranges, all reads goes to normal endpoints;
    // read_new_t::yes - triggers update_pending_ranges to compute and store new ranges for read requests.
    // The value is preserved in all clone functions, the default is read_new_t::no.
    using read_new_t = bool_class<class read_new_tag>;
    void set_read_new(read_new_t value);

    /** @return an endpoint to token multimap representation of tokenToEndpointMap (a copy) */
    std::multimap<inet_address, token> get_endpoint_to_token_map_for_reading() const;
    /**
     * @return a (stable copy, won't be modified) Token to Endpoint map for all the normal and bootstrapping nodes
     *         in the cluster.
     */
    std::map<token, inet_address> get_normal_and_bootstrapping_token_to_endpoint_map() const;

    long get_ring_version() const;
    void invalidate_cached_rings();

    version_t get_version() const;
    void set_version(version_t version);

    friend class token_metadata_impl;
    friend class shared_token_metadata;
private:
    void set_version_tracker(version_tracker_t tracker);
};

struct topology_change_info {
    token_metadata target_token_metadata;
    std::optional<token_metadata> base_token_metadata;
    std::vector<dht::token> all_tokens;
    token_metadata::read_new_t read_new;

    topology_change_info(token_metadata target_token_metadata_,
        std::optional<token_metadata> base_token_metadata_,
        std::vector<dht::token> all_tokens_,
        token_metadata::read_new_t read_new_);
    future<> clear_gently();
};

using token_metadata_ptr = lw_shared_ptr<const token_metadata>;
using mutable_token_metadata_ptr = lw_shared_ptr<token_metadata>;
using token_metadata_lock = semaphore_units<>;
using token_metadata_lock_func = noncopyable_function<future<token_metadata_lock>() noexcept>;

template <typename... Args>
mutable_token_metadata_ptr make_token_metadata_ptr(Args... args) {
    return make_lw_shared<token_metadata>(std::forward<Args>(args)...);
}

class shared_token_metadata {
    mutable_token_metadata_ptr _shared;
    token_metadata_lock_func _lock_func;

    // We use this barrier during the transition to a new token_metadata version to ensure that the
    // system stops using previous versions. Here are the key points:
    //   * A new phase begins when a mutable_token_metadata_ptr passed to shared_token_metadata::set has
    //   a higher version than the current one.
    //   * Each shared_token_metadata::set call initiates an operation on the barrier. If multiple calls
    //   have the same version, multiple operations may be initiated with the same phase.
    //   * The operation is stored within the new token_metadata instance (token_metadata::set_version_tracker),
    //   and it completes when the instance is destroyed.
    //   * The method shared_token_metadata::stale_versions_in_use can be used to wait for the phase
    //   transition to complete. Once this future resolves, there will be no token_metadata instances
    //   with versions lower than the current one.
    //   * Multiple new phases (version upgrades) can be started before accessing stale_versions_in_use.
    //   However, stale_versions_in_use waits for all previous phases to finish, as advance_and_await
    //   includes its own invocation as an operation in the new phase.
    utils::phased_barrier _versions_barrier;
    shared_future<> _stale_versions_in_use{make_ready_future<>()};
    token_metadata::version_t _fence_version = 0;

public:
    // used to construct the shared object as a sharded<> instance
    // lock_func returns semaphore_units<>
    explicit shared_token_metadata(token_metadata_lock_func lock_func, token_metadata::config cfg)
        : _shared(make_token_metadata_ptr(std::move(cfg)))
        , _lock_func(std::move(lock_func))
    {
        _shared->set_version_tracker(_versions_barrier.start());
    }

    shared_token_metadata(const shared_token_metadata& x) = delete;
    shared_token_metadata(shared_token_metadata&& x) = default;

    token_metadata_ptr get() const noexcept {
        return _shared;
    }

    void set(mutable_token_metadata_ptr tmptr) noexcept;

    future<> stale_versions_in_use() const {
        return _stale_versions_in_use.get_future();
    }

    void update_fence_version(token_metadata::version_t version);
    token_metadata::version_t get_fence_version() const noexcept {
        return _fence_version;
    }

    // Token metadata changes are serialized
    // using the schema_tables merge_lock.
    //
    // Must be called on shard 0.
    future<token_metadata_lock> get_lock() const noexcept {
        return _lock_func();
    }

    // mutate_token_metadata_on_all_shards acquires the shared_token_metadata lock,
    // clones the token_metadata (using clone_async)
    // and calls an asynchronous functor on
    // the cloned copy of the token_metadata to mutate it.
    //
    // If the functor is successful, the mutated clone
    // is set back to to the shared_token_metadata,
    // otherwise, the clone is destroyed.
    future<> mutate_token_metadata(seastar::noncopyable_function<future<> (token_metadata&)> func);

    // mutate_token_metadata_on_all_shards acquires the shared_token_metadata lock,
    // clones the token_metadata (using clone_async)
    // and calls an asynchronous functor on
    // the cloned copy of the token_metadata to mutate it.
    //
    // If the functor is successful, the mutated clone
    // is set back to to the shared_token_metadata on all shards,
    // otherwise, the clone is destroyed.
    //
    // Must be called on shard 0.
    static future<> mutate_on_all_shards(sharded<shared_token_metadata>& stm, seastar::noncopyable_function<future<> (token_metadata&)> func);
};

std::unique_ptr<locator::token_range_splitter> make_splitter(token_metadata_ptr);

}
