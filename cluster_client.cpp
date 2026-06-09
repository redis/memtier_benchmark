/*
 * Copyright (C) 2011-2026 Redis Labs Ltd.
 *
 * This file is part of memtier_benchmark.
 *
 * memtier_benchmark is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2.
 *
 * memtier_benchmark is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with memtier_benchmark.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif

#ifdef HAVE_ASSERT_H
#include <assert.h>
#endif

#include "cluster_client.h"
#include "command_meta.h"
#include "memtier_benchmark.h"
#include "obj_gen.h"
#include "retry_policy.h"
#include "shard_connection.h"

#define KEY_INDEX_QUEUE_MAX_SIZE 1000000
#define STAGED_MONITOR_QUEUE_MAX_SIZE 1000000

// After this many consecutive read-routing failures under strict rp_secondary
// (select_target_conn returning UINT_MAX for reads), hold_pipeline yields the
// event loop so the reactor can fire BEV_EVENT_CONNECTED for in-progress
// replica connections instead of busy-spinning through fill_pipeline.
#define STRICT_NO_ROUTE_HOLD_THRESHOLD 64

#define MOVED_MSG_PREFIX "-MOVED"
#define MOVED_MSG_PREFIX_LEN 6
#define ASK_MSG_PREFIX "-ASK"
#define ASK_MSG_PREFIX_LEN 4

#define MAX_CLUSTER_HSLOT 16383
static const uint16_t crc16tab[256] = {
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad,
    0xe1ce, 0xf1ef, 0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6, 0x9339, 0x8318, 0xb37b, 0xa35a,
    0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b,
    0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d, 0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc, 0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861,
    0x2802, 0x3823, 0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b, 0x5af5, 0x4ad4, 0x7ab7, 0x6a96,
    0x1a71, 0x0a50, 0x3a33, 0x2a12, 0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a, 0x6ca6, 0x7c87,
    0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70, 0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a,
    0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3,
    0x5004, 0x4025, 0x7046, 0x6067, 0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1, 0x1290,
    0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405, 0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e,
    0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634, 0xd94c, 0xc96d, 0xf90e, 0xe92f,
    0x99c8, 0x89e9, 0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3, 0xcb7d, 0xdb5c,
    0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a, 0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83,
    0x1ce0, 0x0cc1, 0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74,
    0x2e93, 0x3eb2, 0x0ed1, 0x1ef0};

static inline uint16_t crc16(const char *buf, size_t len)
{
    size_t counter;
    uint16_t crc = 0;
    for (counter = 0; counter < len; counter++)
        crc = (crc << 8) ^ crc16tab[((crc >> 8) ^ *buf++) & 0x00FF];
    return crc;
}

static uint32_t calc_hslot_crc16_cluster(const char *str, size_t length)
{
    uint32_t rv = (uint32_t) crc16(str, length) & MAX_CLUSTER_HSLOT;
    return rv;
}

// Hash-tag-aware variant of calc_hslot_crc16_cluster. Mirrors Redis' rule
// (https://redis.io/docs/reference/cluster-spec/#hash-tags): if the key
// contains a {tag} substring with at least one byte between the braces, only
// the bytes inside the first such {tag} are hashed; otherwise the whole key
// is hashed. memtier's default slot computation skips this rule because the
// generic routing path only sees obj_gen->get_key() — never the literal
// affixes like "{mx}-" that the user wrote in --command. This helper is used
// by --transaction to pin to the actual slot owner.
static uint32_t calc_hslot_crc16_with_hash_tag(const char *str, size_t length)
{
    const char *open = (const char *) memchr(str, '{', length);
    if (open != NULL) {
        size_t remaining = length - (open - str) - 1;
        const char *close = (const char *) memchr(open + 1, '}', remaining);
        if (close != NULL && close > open + 1) {
            size_t tag_len = close - open - 1;
            return (uint32_t) crc16(open + 1, tag_len) & MAX_CLUSTER_HSLOT;
        }
    }
    return (uint32_t) crc16(str, length) & MAX_CLUSTER_HSLOT;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

cluster_client::cluster_client(client_group *group) :
        client(group),
        m_txn_pinned_conn_id(-1),
        m_txn_observed_rotation_seq(0),
        m_txn_staged_key_index(0),
        m_txn_has_staged_key(false),
        m_txn_pin_lost_warned(false),
        m_strict_no_route_attempts(0),
        m_last_no_replica_warning_ts(0)
{
    // Initialize slot map to the UINT_MAX sentinel; m_shard_groups starts empty
    // and is populated by handle_cluster_slots(). Every reader bails on
    // UINT_MAX (slot_primary_conn_id, select_target_conn, get_key_for_conn,
    // retry_after_redirect, create_monitor_request_cluster, create_mget_request),
    // so the pre-bootstrap window cannot silently route to a stale group index.
    // cluster_client::connect() still forces the bootstrap CLUSTER SLOTS before
    // user traffic flows; the sentinel is belt-and-braces against a topology
    // refresh leaving stale entries for slots absent from the new reply.
    m_slot_to_shard_group.assign(MAX_CLUSTER_HSLOT + 1, UINT_MAX);
}

cluster_client::~cluster_client()
{
    for (unsigned int i = 0; i < m_key_index_pools.size(); i++) {
        key_index_pool *key_idx_pool = m_key_index_pools[i];
        delete key_idx_pool;
    }
    m_key_index_pools.clear();
}

int cluster_client::connect(void)
{
    // get main connection
    shard_connection *sc = MAIN_CONNECTION;
    assert(sc != NULL);

    // set main connection to send 'CLUSTER SLOTS' command
    sc->set_cluster_slots();

    // create key index pool for main connection only on the first connect.
    // On reconnects (e.g. via --reconnect-on-error), the main connection's
    // key_index_pool already exists and m_connections is unchanged, so
    // pushing again would break the invariant the assertion enforces.
    if (m_key_index_pools.empty()) {
        key_index_pool *key_idx_pool = new key_index_pool;
        m_key_index_pools.push_back(key_idx_pool);
        m_staged_monitor_commands.emplace_back();
    }
    assert(m_connections.size() == m_key_index_pools.size());
    assert(m_connections.size() == m_staged_monitor_commands.size());

    // continue with base class
    client::connect();

    return 0;
}

void cluster_client::txn_release_pin()
{
    if (m_txn_has_staged_key) {
        // Return the staged key to the pin connection's pool so the next
        // rotation can reuse it. Without this, every mid-rotation pin reset
        // (MOVED, disconnect) silently burns one key from the sequential
        // iterator, permanently skipping that index under --key-pattern=S.
        m_key_index_pools[m_txn_pinned_conn_id]->push(m_txn_staged_key_index);
        m_txn_has_staged_key = false;
    }
    m_txn_pinned_conn_id = -1;
}

void cluster_client::disconnect(void)
{
    // Reset transaction pin state so a post-reconnect topology with fewer
    // shards doesn't leave m_txn_pinned_conn_id pointing past the end of
    // m_connections (which would be an out-of-bounds access).
    m_txn_pinned_conn_id = -1;
    m_txn_has_staged_key = false;
    m_txn_pin_lost_warned = false;

    unsigned int conn_size = m_connections.size();
    unsigned int i;

    // disconnect all connections
    for (i = 0; i < m_connections.size(); i++) {
        shard_connection *sc = m_connections[i];
        sc->disconnect();
    }

    // delete all connections except main connection
    for (i = conn_size - 1; i > 0; i--) {
        shard_connection *sc = m_connections.back();
        m_connections.pop_back();
        delete sc;
        // m_key_index_pools and m_staged_monitor_commands are intentionally NOT shrunk:
        // their entries are cleared by connect_shard_connection() on the next reconnect.
        // Keeping them here avoids a size divergence between the two parallel vectors
        // (which would fire the assert in create_shard_connection on re-connect).
    }
}

shard_connection *cluster_client::create_shard_connection(abstract_protocol *abs_protocol)
{
    shard_connection *sc = new shard_connection(m_connections.size(), this, m_config, m_event_base, abs_protocol);
    assert(sc != NULL);

    m_connections.push_back(sc);

    // create key index pool
    key_index_pool *key_idx_pool = new key_index_pool;
    assert(key_idx_pool != NULL);

    m_key_index_pools.push_back(key_idx_pool);
    m_staged_monitor_commands.emplace_back();
    assert(m_connections.size() == m_key_index_pools.size());
    assert(m_connections.size() == m_staged_monitor_commands.size());

    return sc;
}

bool cluster_client::connect_shard_connection(shard_connection *sc, char *address, char *port)
{
    // empty key index queue and staged monitor commands
    if (m_key_index_pools[sc->get_id()]->size()) {
        // Cross-shard reads that were routed to this connection (via
        // create_request_for_other) push a (command_index, key_index) pair into
        // m_key_index_pools[sc->get_id()] AFTER client::create_request has
        // already incremented m_reqs_generated (client.cpp:656 for GET, the
        // arbitrary path at create_request_for_other for --command). Clearing
        // the pool here without compensating leaves m_reqs_processed < m_reqs_generated
        // permanently, so a --requests run hangs and a --test-time run
        // mis-accounts pending in-flight. Compensate by n/2 (pairs).
        // Defensive clamp guards against underflow from any future single-entry
        // push pattern (txn_release_pin pushes a lone key, but the pin path
        // does not increment m_reqs_generated for that staged key -- it is
        // consumed by the next rotation -- so n/2 is the right accounting
        // for cross-shard residue and a no-op for the txn-staged singleton
        // beyond the clamp.) Matches the pattern at hold_pipeline.
        key_index_pool empty_queue;
        std::swap(*m_key_index_pools[sc->get_id()], empty_queue);
        {
            const size_t n = empty_queue.size() / 2;
            m_reqs_generated -= (m_reqs_generated >= n) ? n : m_reqs_generated;
        }
    }
    {
        std::queue<staged_monitor_cmd> empty_staged;
        std::swap(m_staged_monitor_commands[sc->get_id()], empty_staged);
        // Commands in the cleared staged queue were already counted in m_reqs_generated at
        // staging time. Compensate so a --requests run is not left waiting for responses
        // that will never arrive.
        // Defensive clamp: today entries in empty_staged are popped before m_reqs_generated
        // is decremented so underflow cannot occur, but guard explicitly so a future
        // refactor does not silently wrap to 2^64.  Matches the pattern at hold_pipeline.
        {
            const size_t n = empty_staged.size();
            m_reqs_generated -= (m_reqs_generated >= n) ? n : m_reqs_generated;
        }
    }

    // save address and port
    sc->set_address_port(address, port);

    // get address information
    struct connect_info ci;
    struct addrinfo *addr_info;
    struct addrinfo hints;

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;

    int res = getaddrinfo(address, port, &hints, &addr_info);
    if (res != 0) {
        benchmark_error_log("connect: resolve error: %s\n", gai_strerror(res));
        return false;
    }

    ci.ci_family = addr_info->ai_family;
    ci.ci_socktype = addr_info->ai_socktype;
    ci.ci_protocol = addr_info->ai_protocol;
    assert(addr_info->ai_addrlen <= sizeof(ci.addr_buf));
    memcpy(ci.addr_buf, addr_info->ai_addr, addr_info->ai_addrlen);
    ci.ci_addr = (struct sockaddr *) ci.addr_buf;
    ci.ci_addrlen = addr_info->ai_addrlen;

    freeaddrinfo(addr_info);

    // call connect
    res = sc->connect(&ci);

    return res == 0;
}

void cluster_client::build_mget_slot_cache()
{
    if (!m_config->multi_key_get) return;

    mget_slot_cache *cache = m_config->mget_cache;
    assert(cache != NULL);

    unsigned int num_conns = (unsigned int) m_connections.size();

    // Slot→key mapping is topology-independent: build it once across all threads.
    pthread_mutex_lock(&cache->mutex);
    if (!cache->built.load(std::memory_order_relaxed)) {
        unsigned long long key_min = m_config->key_minimum;
        unsigned long long key_max = m_config->key_maximum;

        // Cap per-slot storage: multi_key_get * 4, bounded to [multi_key_get, 4096].
        // This bounds both memory and scan time regardless of key range size.
        unsigned int cap = (unsigned int) m_config->multi_key_get * 4;
        if (cap > 4096) cap = 4096;
        if (cap < (unsigned int) m_config->multi_key_get) cap = (unsigned int) m_config->multi_key_get;

        benchmark_error_log("Building MGET slot cache for key range [%llu, %llu] "
                            "(cap %u keys/slot)...\n",
                            key_min, key_max, cap);

        cache->slot_keys.assign(MAX_CLUSTER_HSLOT + 1, std::vector<unsigned long long>());

        unsigned int filled_slots = 0;
        for (unsigned long long idx = key_min; idx <= key_max && filled_slots < MAX_CLUSTER_HSLOT + 1; idx++) {
            m_obj_gen->generate_key(idx);
            unsigned int slot = calc_hslot_crc16_with_hash_tag(m_obj_gen->get_key(), m_obj_gen->get_key_len());
            if (cache->slot_keys[slot].size() < cap) {
                cache->slot_keys[slot].push_back(idx);
                if (cache->slot_keys[slot].size() == cap) filled_slots++;
            }
        }

        cache->built.store(true, std::memory_order_release);

        // Count slots that ended up with at least one key (informational).
        unsigned int populated = 0;
        for (unsigned int s = 0; s <= MAX_CLUSTER_HSLOT; s++) {
            if (!cache->slot_keys[s].empty()) populated++;
        }
        benchmark_error_log("MGET slot cache built: %u/%u slots populated.\n", populated, MAX_CLUSTER_HSLOT + 1);
    }
    pthread_mutex_unlock(&cache->mutex);

    // Per-thread cursor: one entry per slot, sized to match the shared table.
    m_mget_slot_cursor.assign(MAX_CLUSTER_HSLOT + 1, 0);

    // Conn→slot mapping depends on topology: rebuild on every refresh.
    m_mget_conn_slots.assign(num_conns, std::vector<unsigned int>());
    m_mget_conn_slot_cursor.assign(num_conns, 0);

    for (unsigned int slot = 0; slot <= MAX_CLUSTER_HSLOT; slot++) {
        if (cache->slot_keys[slot].empty()) continue;
        // Resolve slot -> shard_group -> primary -> conn_id. Behavior identical
        // to the prior `m_slot_to_shard[slot]` lookup; the indirection through
        // m_shard_groups is the prep for replica-aware routing.
        unsigned int gidx = m_slot_to_shard_group[slot];
        if (gidx >= m_shard_groups.size()) continue;
        shard_connection *primary = m_shard_groups[gidx].primary;
        if (primary == NULL) continue;
        unsigned int cid = primary->get_id();
        if (cid < num_conns) m_mget_conn_slots[cid].push_back(slot);

        // Slot ownership is intentionally primary-only: exactly one producer
        // per shard. The primary's fill_pipeline ticks drive create_mget_request,
        // which then calls select_target_conn(slot, true) to honor
        // --read-preference and dispatch the MGET to the chosen primary or
        // replica connection. Attaching the same slot list to replica conns as
        // well caused duplicate MGET issuance (the replica's own fill_pipeline
        // would also produce an MGET for "its" slot, and m_get_ratio_count —
        // shared at the client level — would advance twice per cycle, so the
        // configured --ratio no longer matched on-the-wire request counts).
        // Replica conns with an empty slot list take the "exhausted" branch
        // in create_mget_request, which resets the SET/GET cycle so mixed
        // --ratio workloads keep producing SETs (routed to the primary) from
        // every connection.
    }
}

unsigned int cluster_client::slot_primary_conn_id(unsigned int slot) const
{
    if (slot >= m_slot_to_shard_group.size()) return UINT_MAX;
    unsigned int gidx = m_slot_to_shard_group[slot];
    if (gidx >= m_shard_groups.size()) return UINT_MAX;
    shard_connection *primary = m_shard_groups[gidx].primary;
    if (primary == NULL) return UINT_MAX;
    return primary->get_id();
}

// True if `sc` is in a usable steady state for sending a fresh user-level
// request: TCP connected, cluster-slots ladder finished, and (for replicas)
// the READONLY ladder also finished. Delegates to shard_connection::
// is_ready_for_reads() which encodes the full per-role readiness predicate.
static inline bool conn_is_live_for_routing(shard_connection *sc)
{
    if (sc == NULL) return false;
    return sc->is_ready_for_reads();
}

unsigned int cluster_client::select_target_conn(unsigned int slot, bool is_read)
{
    if (slot >= m_slot_to_shard_group.size()) return UINT_MAX;
    unsigned int gidx = m_slot_to_shard_group[slot];
    if (gidx >= m_shard_groups.size()) return UINT_MAX;
    shard_group &group = m_shard_groups[gidx];
    if (group.primary == NULL) return UINT_MAX;

    // Writes always go to the primary. This is the only sane choice; replicas
    // either reject (-READONLY) or accept and immediately diverge from the
    // authoritative primary.
    //
    // Writes only require the primary's TCP socket to be connected; the full
    // is_ready_for_reads() predicate (which also requires
    // m_cluster_slots == setup_done) is unnecessarily strict here. Several
    // code paths transiently flip the primary's m_cluster_slots back to
    // setup_none (role-aware disconnect, MOVED redirect, READONLY-no-loop
    // guard, the build-then-swap window of a CLUSTER SLOTS refresh). During
    // those windows is_ready_for_reads() returns false even though the slot
    // map is still valid and dispatching a SET on the producer's own slot
    // will not loop. Because client::get_key_for_conn has already advanced
    // m_obj_gen->m_next_key by the time we get here, returning UINT_MAX
    // *consumes* the key index without issuing the SET -- and when the
    // iterator wraps at m_key_max the client re-writes already-touched
    // keys, manifesting as a deterministic per-run key shortfall
    // (observed pattern: ~9 skips x 20 clients = ~176 keys; 500000 == 499824).
    //
    // The cluster_slots gate is meaningful only for reads, which need a
    // valid slot map to ROUTE; writes to the producer's primary do not
    // re-look-up routing. Reads continue to use the full
    // conn_is_live_for_routing() / is_ready_for_reads() predicate below.
    //
    // Round-13: even the conn_connected gate has been removed for writes.
    // The producer's own slot is conn_connected by definition (fill_pipeline
    // runs only on connected conns). For cross-shard writes, the target's
    // own fill_pipeline waits for setup_done before draining its pool, so
    // queueing here is safe regardless of target TCP state. The pre-PR
    // master path returned available_for_conn unconditionally for
    // producer==primary; this restores that invariant for both
    // producer-owned and cross-shard writes. Gating on transient
    // conn_in_progress/conn_disconnected (bootstrap or post-reconnect)
    // silently consumed the key index advanced by client::get_key_for_conn
    // and was the root cause of the 500000 == 499824 keyspace loss.
    if (!is_read) {
        return group.primary->get_id();
    }

    const enum read_pref_mode mode = m_config->read_preference;
    // Used by both rp_nearest (cold-seed RR) and rp_secondary/_preferred (live
    // replica RR). Hoisted above the mode switch so the two branches share one
    // declaration instead of shadowing each other.
    const size_t nreplicas = group.replicas.size();

    // rp_primary: legacy behavior (read from primary). Falling all the way
    // through to the slot's primary owner gives parity with the
    // pre-read-preference world.
    if (mode == rp_primary) {
        return conn_is_live_for_routing(group.primary) ? group.primary->get_id() : UINT_MAX;
    }

    // rp_nearest: scan warm replicas (and the primary if it has already
    // accumulated samples) and pick the lowest EWMA. Cold (samples <
    // threshold) entries are treated as +inf so the tiebreak is among warm
    // endpoints. While any live replica is still cold, route to it round-
    // robin so every replica accumulates its first LATENCY_EWMA_MIN_SAMPLES;
    // once all live replicas are warm, the EWMA pick below takes over.
    // (Consider primary first - under built-in GET/MGET workloads it never
    // warms because no rt_get response reaches it, but under arbitrary
    // mixed workloads rt_arbitrary writes update its EWMA, so it can
    // participate in selection. The no-live-replica fallback below also
    // routes traffic to primary, which independently warms it.)
    if (mode == rp_nearest) {
        shard_connection *best = NULL;
        double best_ewma = 0.0;
        // Consider primary first (contends only if it has accumulated
        // samples via rt_arbitrary writes or the no-live-replica fallback;
        // see comment above).
        if (conn_is_live_for_routing(group.primary) && group.primary->latency_ewma_warm()) {
            best = group.primary;
            best_ewma = group.primary->get_latency_ewma_us();
        }
        bool any_cold_live_replica = false;
        for (size_t i = 0; i < nreplicas; i++) {
            shard_connection *r = group.replicas[i];
            if (!conn_is_live_for_routing(r)) continue;
            if (!r->latency_ewma_warm()) {
                any_cold_live_replica = true;
                continue;
            }
            const double e = r->get_latency_ewma_us();
            if (best == NULL || e < best_ewma) {
                best = r;
                best_ewma = e;
            }
        }
        // Seed cold live replicas first: round-robin over the replica list
        // looking for any live-but-not-yet-warm replica. Once everyone is
        // warm, this loop finds nothing and we fall through to either the
        // warm pick (`best`) or the primary fallback.
        if (any_cold_live_replica && nreplicas > 0) {
            for (size_t step = 0; step < nreplicas; step++) {
                unsigned int idx = (group.replica_rr_cursor + step) % nreplicas;
                shard_connection *r = group.replicas[idx];
                if (conn_is_live_for_routing(r) && !r->latency_ewma_warm()) {
                    group.replica_rr_cursor = (idx + 1) % nreplicas;
                    return r->get_id();
                }
            }
        }
        if (best != NULL) return best->get_id();
        // No warm endpoint and no cold live replica — keep traffic on the
        // primary so reads still flow during the warm-up window.
        return conn_is_live_for_routing(group.primary) ? group.primary->get_id() : UINT_MAX;
    }

    // rp_secondary / rp_secondary_preferred: round-robin over live replicas.
    // The cursor lives on the per-thread shard_group and wraps; we walk up
    // to N entries so a single dead replica doesn't push us back to primary.
    if (nreplicas > 0) {
        for (size_t step = 0; step < nreplicas; step++) {
            unsigned int idx = (group.replica_rr_cursor + step) % nreplicas;
            shard_connection *r = group.replicas[idx];
            if (conn_is_live_for_routing(r)) {
                group.replica_rr_cursor = (idx + 1) % nreplicas;
                return r->get_id();
            }
        }
    }

    // No live replica. rp_secondary_preferred falls back to the primary;
    // rp_secondary returns UINT_MAX and lets the caller apply
    // --read-preference-fallback (rpf_error / rpf_queue / rpf_primary).
    if (mode == rp_secondary_preferred) {
        return conn_is_live_for_routing(group.primary) ? group.primary->get_id() : UINT_MAX;
    }
    // Strict rp_secondary: honor --read-preference-fallback at the routing
    // site. rpf_primary silently degrades to the primary; rpf_error and
    // rpf_queue both return UINT_MAX (caller treats as not_available).
    if (m_config->read_preference_fallback == rpf_primary) {
        return conn_is_live_for_routing(group.primary) ? group.primary->get_id() : UINT_MAX;
    }
    return UINT_MAX;
}

bool cluster_client::classify_read(const request *req) const
{
    if (req == NULL) return false;
    switch (req->m_type) {
    case rt_get:
        return true;
    case rt_set:
    case rt_wait: // WAIT must run on the primary; classify as write
    case rt_auth:
    case rt_select_db:
    case rt_cluster_slots:
    case rt_hello:
    case rt_readonly:
        return false;
    case rt_arbitrary: {
        const arbitrary_request *ar = static_cast<const arbitrary_request *>(req);
        if (ar->m_cmd_meta == NULL) return false;
        // Per-command override takes precedence over the command-meta lookup.
        if (ar->m_cmd_meta->is_read_override == 1) return true;
        if (ar->m_cmd_meta->is_read_override == 0) return false;
        // Spec-resolved is_read flag (READONLY in Redis command-flags).
        if (ar->m_cmd_meta->spec != NULL) return ar->m_cmd_meta->spec->is_read;
        return false;
    }
    case rt_unknown:
    default:
        return false;
    }
}

void cluster_client::record_read_routing(size_t arbitrary_index, bool from_replica)
{
    if (m_arbitrary_routing_counters.empty()) {
        // Lazily size to match the configured --command count. handle_response
        // can land here before run_stats finishes its own per-command setup,
        // so size on first use.
        if (m_config && m_config->arbitrary_commands) {
            m_arbitrary_routing_counters.assign(m_config->arbitrary_commands->size(), read_routing_counters());
        }
    }
    if (arbitrary_index >= m_arbitrary_routing_counters.size()) return;
    if (from_replica)
        m_arbitrary_routing_counters[arbitrary_index].ops_from_replica++;
    else
        m_arbitrary_routing_counters[arbitrary_index].ops_from_primary++;
}

void cluster_client::record_builtin_read_routing(request_type rt, bool from_replica)
{
    if (rt != rt_get) return;
    if (from_replica)
        m_get_routing_counters.ops_from_replica++;
    else
        m_get_routing_counters.ops_from_primary++;
}

bool cluster_client::handle_cluster_slots(protocol_response *r)
{
    /*
     * temporary array to test if some of the connections are left with no
     * slots, and need to be closed.
     */
    unsigned long prev_connections_size = m_connections.size();
    std::vector<bool> close_sc(prev_connections_size, true);

    // Validate the top-level reply shape before walking it. as_mbulk_size()
    // and as_bulk() both call assert(0) on type mismatch, and bare
    // mbulks_elements[N] indexing is UB past-end. A malformed CLUSTER SLOTS
    // reply from a misbehaving / hostile server (#417: fixture
    // `cluster_slots_malformed.bin` from #409) hit both. Drop malformed
    // shards instead of crashing; if the entire reply is unusable, the
    // existing bootstrap connection stays in service.
    if (r->get_mbulk_value() == NULL) {
        benchmark_error_log("warning: CLUSTER SLOTS: server returned non-array; ignoring reply\n");
        return false;
    }

    // A *valid* zero-shard reply would silently retire every existing
    // connection (the close_sc[] loop further down). That's worse than
    // crashing -- the benchmark continues with no shards. Reject it.
    if (r->get_mbulk_value()->mbulks_elements.size() == 0) {
        benchmark_error_log("warning: CLUSTER SLOTS: server returned empty topology; ignoring reply\n");
        return false;
    }

    // Track whether any shard in the reply passed validation. If every shard
    // is malformed and we fall through the loop with no `close_sc[j] = false`
    // anywhere, the close-stale-connections pass below would tear down EVERY
    // existing connection (including the bootstrap), which contradicts the
    // documented "bootstrap stays in service" invariant. (Cursor bugbot.)
    bool any_valid_shard = false;

    // Build the new topology into LOCAL buffers (build-then-swap pattern).
    //
    // Parse into new_slot_map / new_groups locals.  Only swap into the member
    // variables if at least one valid shard was produced.  On an all-skipped
    // reply the member state is untouched and a warning is emitted, so traffic
    // continues to route against the prior topology rather than seeing an
    // empty slot map until the next successful refresh.  The stale-index
    // aliasing invariant (slots absent from the new reply must map to
    // UINT_MAX, not to a reused group index) is preserved because
    // new_slot_map is initialised to UINT_MAX and is only written for slots
    // explicitly listed in a valid shard range -- identical to an
    // unconditional assign(), just deferred until commit time.
    //
    // Connections (m_connections) are still created/reused in-place during the
    // parse loop; in the all-malformed case no new connections are created
    // (every shard is skipped before reaching create_shard_connection), so
    // there is nothing to roll back on that side.
    assert(m_slot_to_shard_group.size() == (size_t) MAX_CLUSTER_HSLOT + 1);
    std::vector<unsigned int> new_slot_map(MAX_CLUSTER_HSLOT + 1, UINT_MAX);
    std::vector<shard_group> new_groups;

    // Pre-scan all shard tuples to collect the set of (addr, port) endpoints
    // advertised as a PRIMARY anywhere in this reply. The replica-registration
    // loop below already guards against flipping a node that's a primary in
    // an EARLIER or the CURRENT shard (via new_groups + `sc`), but it cannot
    // see LATER shards that haven't been parsed yet. Without this pre-scan a
    // node that serves as replica in shard N and primary in shard M > N would
    // still get role_replica + rearm_readonly() applied at the replica loop,
    // and the role flip would silently win until shard M is processed (which
    // re-sets role_primary at line 803 but does NOT undo rearm_readonly).
    // The known-gap comment at lines 921-929 documented exactly this.
    // Bugbot HIGH iter6-R3 / round-29 cursor[bot] thread.
    //
    // Shape this as a vector<pair<string,string>> rather than a set so a
    // malformed shard tuple (caught by the validation below) can still
    // contribute defensively; we only consult contains(), and false negatives
    // are harmless because the existing guards still cover earlier shards.
    std::vector<std::pair<std::string, std::string> > advertised_primary_endpoints;
    for (unsigned int pi = 0; pi < r->get_mbulk_value()->mbulks_elements.size(); pi++) {
        mbulk_element *ps_el = r->get_mbulk_value()->mbulks_elements[pi];
        if (ps_el == NULL || !ps_el->is_mbulk_size()) continue;
        mbulk_size_el *ps = ps_el->as_mbulk_size();
        if (ps->mbulks_elements.size() < 3 || !ps->mbulks_elements[2]->is_mbulk_size()) continue;
        mbulk_size_el *pn = ps->mbulks_elements[2]->as_mbulk_size();
        if (pn->mbulks_elements.size() < 2 || !pn->mbulks_elements[0]->is_bulk() || !pn->mbulks_elements[1]->is_bulk())
            continue;
        bulk_el *pa = pn->mbulks_elements[0]->as_bulk();
        bulk_el *pp = pn->mbulks_elements[1]->as_bulk();
        if (pa->value_len == 0 || pp->value_len == 0) continue;
        if (memchr(pa->value, '\0', pa->value_len) != NULL) continue;
        // pp->value points at the bulk header (':' integer-reply prefix);
        // value+1 / value_len-1 strips it, matching the primary-port copy
        // at lines 754-757.
        std::string p_addr((const char *) pa->value, pa->value_len);
        std::string p_port((const char *) pp->value + 1, pp->value_len - 1);
        advertised_primary_endpoints.push_back(std::make_pair(p_addr, p_port));
    }

    // run over response and create connections
    for (unsigned int i = 0; i < r->get_mbulk_value()->mbulks_elements.size(); i++) {
        mbulk_element *shard_el = r->get_mbulk_value()->mbulks_elements[i];
        if (shard_el == NULL || !shard_el->is_mbulk_size()) {
            benchmark_error_log("warning: CLUSTER SLOTS: shard %u not an array; skipping\n", i);
            continue;
        }
        mbulk_size_el *shard = shard_el->as_mbulk_size();
        if (shard->mbulks_elements.size() < 3 || !shard->mbulks_elements[0]->is_bulk() ||
            !shard->mbulks_elements[1]->is_bulk() || !shard->mbulks_elements[2]->is_mbulk_size()) {
            benchmark_error_log(
                "warning: CLUSTER SLOTS: shard %u malformed (need [start, end, [host, port, ...]]); skipping\n", i);
            continue;
        }

        // Slot bounds: must be parseable, in [0, MAX_CLUSTER_HSLOT], and
        // min <= max. The old code took the strtol result verbatim and
        // wrote into m_slot_to_shard[min..max] -- a hostile server could
        // make us write past the end of the 16384-sized array (OOB write).
        bulk_el *min_el = shard->mbulks_elements[0]->as_bulk();
        bulk_el *max_el = shard->mbulks_elements[1]->as_bulk();
        if (min_el->value_len == 0 || max_el->value_len == 0) {
            benchmark_error_log("warning: CLUSTER SLOTS: shard %u empty slot bound; skipping\n", i);
            continue;
        }
        errno = 0;
        long parsed_min = strtol(min_el->value + 1, NULL, 10);
        long parsed_max = strtol(max_el->value + 1, NULL, 10);
        if (errno == ERANGE || parsed_min < 0 || parsed_max < 0 || parsed_min > MAX_CLUSTER_HSLOT ||
            parsed_max > MAX_CLUSTER_HSLOT || parsed_min > parsed_max) {
            benchmark_error_log("warning: CLUSTER SLOTS: shard %u slot range [%ld, %ld] out of [0, %d]; skipping\n", i,
                                parsed_min, parsed_max, MAX_CLUSTER_HSLOT);
            continue;
        }
        int min_slot = (int) parsed_min;
        int max_slot = (int) parsed_max;

        mbulk_size_el *node = shard->mbulks_elements[2]->as_mbulk_size();
        if (node->mbulks_elements.size() < 2 || !node->mbulks_elements[0]->is_bulk() ||
            !node->mbulks_elements[1]->is_bulk()) {
            benchmark_error_log("warning: CLUSTER SLOTS: shard %u node tuple malformed (need host, port); skipping\n",
                                i);
            continue;
        }

        // hostname/ip + port: reject zero-length bulks (memcpy(..., NULL+1, 0)
        // is technically UB; embedded NULs would also alias other addrs in
        // strcmp-based lookup).
        bulk_el *mbulk_addr_el = node->mbulks_elements[0]->as_bulk();
        bulk_el *mbulk_port_el = node->mbulks_elements[1]->as_bulk();
        if (mbulk_addr_el->value_len == 0 || mbulk_port_el->value_len == 0) {
            benchmark_error_log("warning: CLUSTER SLOTS: shard %u empty host/port; skipping\n", i);
            continue;
        }
        if (memchr(mbulk_addr_el->value, '\0', mbulk_addr_el->value_len) != NULL) {
            benchmark_error_log("warning: CLUSTER SLOTS: shard %u host contains NUL; skipping\n", i);
            continue;
        }

        char *addr = (char *) malloc(mbulk_addr_el->value_len + 1);
        memcpy(addr, mbulk_addr_el->value, mbulk_addr_el->value_len);
        addr[mbulk_addr_el->value_len] = '\0';

        // value points at the bulk header byte (':' for integer reply) and
        // value_len is the strdup'd length INCLUDING that prefix. value+1
        // skips the prefix; the digits themselves are value_len-1 bytes.
        // The old form copied value_len bytes and worked only because the
        // strdup() trailing NUL backed up the +1 overrun. Match the source
        // length to the intent so static analyzers stop flagging this.
        const unsigned int port_digits_len = mbulk_port_el->value_len - 1;
        char *port = (char *) malloc(port_digits_len + 1);
        memcpy(port, mbulk_port_el->value + 1, port_digits_len);
        port[port_digits_len] = '\0';

        // check if connection already exist
        shard_connection *sc = NULL;
        unsigned int j;

        for (j = 0; j < m_connections.size(); j++) {
            if (strcmp(addr, m_connections[j]->get_address()) == 0 && strcmp(port, m_connections[j]->get_port()) == 0) {
                sc = m_connections[j];

                // mark not to close this connection
                if (j < prev_connections_size) close_sc[j] = false;

                // if connection disconnected, try to reconnect
                if (sc->get_connection_state() == conn_disconnected) {
                    // Role must be set BEFORE connect() so the setup ladder is
                    // armed against the *new* role (primary). A reused endpoint
                    // that was previously labeled role_replica would otherwise
                    // re-arm the READONLY ladder against the now-primary node,
                    // causing the handshake to send READONLY to a primary and
                    // leaving the connection in a state where writes route to
                    // it as group.primary while it still presents replica-side
                    // setup state. Mirrors the set_role-before-connect order
                    // used for replicas below.
                    sc->set_role(role_primary);
                    connect_shard_connection(sc, addr, port);
                }

                break;
            }
        }

        // if connection doesn't exist, add it
        if (sc == NULL) {
            sc = create_shard_connection(MAIN_CONNECTION->get_protocol());
            // Set the role before connect() so the setup ladder is armed at
            // connect time (primary path: no READONLY ladder).
            sc->set_role(role_primary);
            connect_shard_connection(sc, addr, port);
        }

        // The primary discovered for this shard owns the slot range; track it
        // here and reuse the same conn for replicas below to attach to.
        // set_role is idempotent; already-correct for newly-created and
        // reconnected primaries; covers the already-connected role-flip case
        // where the previous label was role_replica.
        sc->set_role(role_primary);

        free(addr);
        free(port);

        // Start a new shard_group for this primary. id == index in
        // new_groups (committed to m_shard_groups on success); consumers
        // (e.g. m_slot_to_shard_group[s]) refer to the group by this index.
        shard_group group;
        group.id = (unsigned int) new_groups.size();
        group.primary = sc;
        group.replica_rr_cursor = 0;

        // Walk replica node tuples at mbulks_elements[3..N]. Same shape as the
        // primary node (host, port, node-id, ...). The dedupe-by-(addr,port)
        // loop below reuses any existing shard_connection so an existing
        // replica that's still in m_connections is preserved across topology
        // refreshes (and is not closed by the close_sc[] sweep).
        for (unsigned int n = 3; n < shard->mbulks_elements.size(); n++) {
            mbulk_element *replica_el = shard->mbulks_elements[n];
            if (replica_el == NULL || !replica_el->is_mbulk_size()) {
                benchmark_error_log("warning: CLUSTER SLOTS: shard %u replica %u not an array; skipping\n", i, n - 3);
                continue;
            }
            mbulk_size_el *replica_node = replica_el->as_mbulk_size();
            if (replica_node->mbulks_elements.size() < 2 || !replica_node->mbulks_elements[0]->is_bulk() ||
                !replica_node->mbulks_elements[1]->is_bulk()) {
                benchmark_error_log(
                    "warning: CLUSTER SLOTS: shard %u replica %u tuple malformed (need host, port); skipping\n", i,
                    n - 3);
                continue;
            }
            bulk_el *r_addr_el = replica_node->mbulks_elements[0]->as_bulk();
            bulk_el *r_port_el = replica_node->mbulks_elements[1]->as_bulk();
            if (r_addr_el->value_len == 0 || r_port_el->value_len == 0) {
                benchmark_error_log("warning: CLUSTER SLOTS: shard %u replica %u empty host/port; skipping\n", i,
                                    n - 3);
                continue;
            }
            if (memchr(r_addr_el->value, '\0', r_addr_el->value_len) != NULL) {
                benchmark_error_log("warning: CLUSTER SLOTS: shard %u replica %u host contains NUL; skipping\n", i,
                                    n - 3);
                continue;
            }

            char *r_addr = (char *) malloc(r_addr_el->value_len + 1);
            memcpy(r_addr, r_addr_el->value, r_addr_el->value_len);
            r_addr[r_addr_el->value_len] = '\0';

            // Same +1 prefix accounting as the primary port copy above:
            // value_len includes the ':' bulk header, so the digit run is
            // value_len-1 bytes.
            const unsigned int r_port_digits_len = r_port_el->value_len - 1;
            char *r_port = (char *) malloc(r_port_digits_len + 1);
            memcpy(r_port, r_port_el->value + 1, r_port_digits_len);
            r_port[r_port_digits_len] = '\0';

            shard_connection *rsc = NULL;
            for (unsigned int k = 0; k < m_connections.size(); k++) {
                if (strcmp(r_addr, m_connections[k]->get_address()) == 0 &&
                    strcmp(r_port, m_connections[k]->get_port()) == 0) {
                    rsc = m_connections[k];

                    if (k < prev_connections_size) close_sc[k] = false;

                    // Defensive guard for shared endpoints: if this same conn
                    // was already claimed as a primary by an earlier shard in
                    // this CLUSTER SLOTS reply, do NOT overwrite the role to
                    // role_replica or rearm READONLY. Standard Redis OSS
                    // topologies don't produce a node that serves as primary
                    // for one shard and replica for another, but exotic /
                    // migrating layouts can. Without this guard the replica-
                    // loop's set_role(role_replica) silently undoes the
                    // primary role established ~80 lines earlier, breaking
                    // routing on the primary shard. Closes the unresolved
                    // Cursor Bugbot HIGH thread.
                    bool already_primary = false;
                    unsigned int primary_group_id = 0;
                    // Cover the current shard's primary too: `sc` was set
                    // ~80 lines above as group.primary for THIS shard but
                    // `group` has not yet been pushed into new_groups (that
                    // happens at the bottom of the outer loop). Without
                    // this check, a replica tuple that reuses the (addr,
                    // port) of its own shard's primary would slip past the
                    // new_groups scan and silently flip the primary's role
                    // to role_replica + rearm READONLY, breaking writes on
                    // that shard. Bugbot HIGH round-26.
                    if (rsc == sc) {
                        already_primary = true;
                        primary_group_id = group.id;
                    }
                    for (size_t gi = 0; !already_primary && gi < new_groups.size(); gi++) {
                        if (new_groups[gi].primary == rsc) {
                            already_primary = true;
                            primary_group_id = new_groups[gi].id;
                            break;
                        }
                    }
                    // Cover LATER shards whose primary tuple hasn't been
                    // parsed yet: consult the pre-scan of advertised primary
                    // endpoints. Without this, a node that's a replica in
                    // shard N and primary in shard M > N would briefly be
                    // role_replica + rearm_readonly() until shard M's tuple
                    // restores role_primary -- but rearm_readonly() flips the
                    // READONLY ladder back to setup_none, so the next read
                    // pass on the now-primary would gate is_ready_for_reads()
                    // and send READONLY to a primary on the next handshake.
                    // Bugbot HIGH iter6-R3 / round-29.
                    if (!already_primary) {
                        for (size_t pi = 0; pi < advertised_primary_endpoints.size(); pi++) {
                            if (advertised_primary_endpoints[pi].first == r_addr &&
                                advertised_primary_endpoints[pi].second == r_port) {
                                already_primary = true;
                                // primary_group_id stays 0 sentinel; the log
                                // line below names addr:port so the operator
                                // can identify the node even without a group
                                // id at this point.
                                break;
                            }
                        }
                    }
                    if (already_primary) {
                        benchmark_error_log("warning: CLUSTER SLOTS: node %s:%s is both primary (shard %u) and replica "
                                            "(shard %u); preserving primary role and skipping replica registration\n",
                                            r_addr, r_port, primary_group_id, group.id);
                        // Skip pushing rsc as a replica for this shard. The
                        // node continues to serve its primary shard correctly.
                        rsc = NULL;
                        break;
                    }

                    // Defensive invariant: by this point the guard at lines
                    // 879-909 must have filtered any rsc that is already a
                    // primary of an earlier shard in this reply (including
                    // the current shard's `sc`). If it didn't, we are about
                    // to silently flip a node from role_primary to
                    // role_replica and rearm READONLY, which is the exact
                    // failure mode the guard exists to prevent. Bugbot HIGH
                    // round-27: assert the contract holds so a future
                    // refactor can't quietly break it.
                    //
                    // Known gap (out of scope here): a LATER shard in the
                    // same reply that claims rsc as ITS primary would be
                    // processed after this replica registration and would
                    // still overwrite the role at line 803. OSS Redis emits
                    // each shard tuple with primary at index [2] before its
                    // replicas, but the reply's outer iteration order
                    // across shards is not guaranteed by the protocol. The
                    // correct fix is a two-pass scan (collect all primaries
                    // first, then walk replicas); tracked as a follow-up.
                    assert(rsc != sc);
                    for (size_t gi = 0; gi < new_groups.size(); gi++) {
                        assert(new_groups[gi].primary != rsc);
                    }
                    if (rsc->get_connection_state() == conn_disconnected) {
                        // Role must be set BEFORE connect() so the READONLY
                        // ladder is armed during the AUTH/HELLO/READONLY/
                        // CLUSTER SLOTS sequence. set_role is a no-op for
                        // already-known replicas (idempotent).
                        rsc->set_role(role_replica);
                        connect_shard_connection(rsc, r_addr, r_port);
                    } else if (rsc->get_role() != role_replica) {
                        // Live role flip: this conn was previously treated as
                        // a primary (m_readonly_state == setup_done with
                        // role_primary) and is now listed as a replica. The
                        // READONLY ladder was never run; rearm_readonly()
                        // re-arms the ladder and sends READONLY immediately so
                        // the server stops rejecting reads with -READONLY.
                        //
                        // Gate this on actual role change: a steady-state
                        // CLUSTER SLOTS refresh of an already-known replica
                        // (rsc->get_role() == role_replica) must NOT re-fire
                        // rearm_readonly. Re-arming flips m_readonly_state
                        // back to setup_none/setup_sent, which gates
                        // is_ready_for_reads() until the next ack, briefly
                        // dropping a stable replica out of read routing on
                        // every periodic topology refresh.
                        rsc->set_role(role_replica);
                        rsc->rearm_readonly();
                    }
                    break;
                }
            }

            if (rsc == NULL) {
                // Either no matching conn was found (new endpoint) or the
                // shared-endpoint guard above forcibly cleared rsc to skip
                // the role overwrite. Distinguish via a re-scan: if a conn
                // with this (addr, port) exists and is already a primary in
                // new_groups, skip creating a new one and skip pushing as a
                // replica.
                bool shared_endpoint_skip = false;
                for (unsigned int k = 0; k < m_connections.size(); k++) {
                    if (strcmp(r_addr, m_connections[k]->get_address()) == 0 &&
                        strcmp(r_port, m_connections[k]->get_port()) == 0) {
                        // Match against the current shard's primary (`sc`)
                        // too, since `group` is not yet in new_groups (see
                        // first-pass guard above). Bugbot HIGH round-26.
                        if (m_connections[k] == sc) {
                            shared_endpoint_skip = true;
                            break;
                        }
                        for (size_t gi = 0; gi < new_groups.size(); gi++) {
                            if (new_groups[gi].primary == m_connections[k]) {
                                shared_endpoint_skip = true;
                                break;
                            }
                        }
                        break;
                    }
                }
                // Cover LATER shards (same iteration-order gap as the
                // existing-conn path above): if any shard in this reply
                // advertises (r_addr, r_port) as primary, do not create a
                // role_replica conn that the later-shard pass will silently
                // role-flip without undoing READONLY. Bugbot HIGH round-29.
                if (!shared_endpoint_skip) {
                    for (size_t pi = 0; pi < advertised_primary_endpoints.size(); pi++) {
                        if (advertised_primary_endpoints[pi].first == r_addr &&
                            advertised_primary_endpoints[pi].second == r_port) {
                            shared_endpoint_skip = true;
                            break;
                        }
                    }
                }
                if (shared_endpoint_skip) {
                    free(r_addr);
                    free(r_port);
                    continue;
                }
                rsc = create_shard_connection(MAIN_CONNECTION->get_protocol());
                // Set the role before connect() so the READONLY ladder is
                // armed at connect time.
                rsc->set_role(role_replica);
                connect_shard_connection(rsc, r_addr, r_port);
                // Defensive invariant: a freshly created rsc cannot already
                // be a primary in this reply (it was just allocated).
                // Same iteration-order caveat as the existing-conn path
                // above. Bugbot HIGH round-27.
                assert(rsc != sc);
                for (size_t gi = 0; gi < new_groups.size(); gi++) {
                    assert(new_groups[gi].primary != rsc);
                }
            }

            group.replicas.push_back(rsc);

            free(r_addr);
            free(r_port);
        }

        // Append the group to the local buffer and remember its index so the
        // slot-range write below points slots at this group rather than at
        // the primary's conn_id directly. (Group index, not conn_id; conn_id
        // is recovered via group.primary->get_id() at lookup time.)
        unsigned int group_idx = group.id;
        new_groups.push_back(group);

        // update range in the local slot map
        for (int j = min_slot; j <= max_slot; j++) {
            new_slot_map[j] = group_idx;
        }

        any_valid_shard = true;
    }

    // If every shard in the reply was malformed and skipped, the local buffers
    // are empty -- leave the existing member state untouched so in-flight
    // traffic continues to route via the previous topology.  (Build-then-swap
    // fix for the eager-reset bug: commit 994e9ad wiped m_slot_to_shard_group
    // and m_shard_groups before parsing, leaving all slots unmapped if parsing
    // produced no valid shard. Cursor bugbot HIGH.)
    if (!any_valid_shard) {
        benchmark_error_log("warning: CLUSTER SLOTS: every shard in the reply was malformed; "
                            "leaving existing connections in service\n");
        return false;
    }

    // At least one valid shard was parsed -- commit the new topology.
    // The stale-index aliasing invariant holds: new_slot_map was initialised
    // to UINT_MAX and slots absent from the reply were never written, so they
    // remain at UINT_MAX in the committed map (same guarantee as the old
    // unconditional assign(), just deferred until here).
    m_slot_to_shard_group.swap(new_slot_map);
    m_shard_groups.swap(new_groups);

    // check if some connections left with no slots, and need to be closed
    for (unsigned int i = 0; i < prev_connections_size; i++) {
        if (close_sc[i] == true) {
            // Flush staged monitor commands unconditionally for any retired shard,
            // regardless of its connection state. A shard can be already disconnected
            // (TCP dropped mid-run) while still holding staged commands that were
            // counted in m_reqs_generated at staging time. hold_pipeline() returns
            // true for disconnected connections so those entries would never self-drain;
            // compensate m_reqs_generated now to prevent a --requests hang.
            if (!m_staged_monitor_commands[i].empty()) {
                std::queue<staged_monitor_cmd> empty_staged;
                std::swap(m_staged_monitor_commands[i], empty_staged);
                // Defensive clamp: entries in empty_staged are popped before
                // m_reqs_generated is decremented so underflow cannot occur today,
                // but guard explicitly so a future refactor does not silently wrap
                // to 2^64.  Matches the pattern at hold_pipeline.
                {
                    const size_t n = empty_staged.size();
                    m_reqs_generated -= (m_reqs_generated >= n) ? n : m_reqs_generated;
                }
            }
            // Same hang for the cross-shard read queue: get_key_for_conn ->
            // create_request_for_other pushes (cmd_idx, key_idx) pairs onto
            // m_key_index_pools[i] AFTER m_reqs_generated was incremented
            // (client.cpp:656). A retired shard's pool would otherwise be
            // discarded by the connection's destructor without compensating
            // the counter, stranding the run. Pair semantics -> divide by 2.
            if (!m_key_index_pools[i]->empty()) {
                key_index_pool empty_queue;
                std::swap(*m_key_index_pools[i], empty_queue);
                {
                    const size_t n = empty_queue.size() / 2;
                    m_reqs_generated -= (m_reqs_generated >= n) ? n : m_reqs_generated;
                }
            }
            if (m_connections[i]->get_connection_state() != conn_disconnected) {
                m_connections[i]->disconnect();
            }
        }
    }

    // Rebuild same-slot key index cache for MGET if enabled.
    build_mget_slot_cache();

    // Wake all connected shard connections so each one re-evaluates hold_pipeline()
    // with the freshly-built m_mget_conn_slots.  Without this, a connection that
    // was bufferevent_disable()'d before the cache existed would never re-run
    // fill_pipeline() and would stay permanently idle.
    if (m_config->multi_key_get > 0) {
        for (size_t i = 0; i < m_connections.size(); i++) {
            if (m_connections[i]->get_connection_state() != conn_disconnected) m_connections[i]->schedule_fill();
        }
    }

    // Topology committed. Tell the caller (shard_connection's CLUSTER SLOTS
    // response handler) it's safe to advance m_cluster_slots to setup_done.
    return true;
}

bool cluster_client::hold_pipeline(unsigned int conn_id)
{
    if (m_connections[conn_id]->get_connection_state() == conn_disconnected) {
        if (m_config->transaction && m_txn_pinned_conn_id == (int) conn_id && !m_txn_pin_lost_warned) {
            m_txn_pin_lost_warned = true;
            benchmark_error_log("warning: --transaction pin connection (id=%u) disconnected mid-rotation; "
                                "transaction stats for the interrupted rotation will be inaccurate.\n",
                                conn_id);
            // Release the pin so non-pin connections can resume the rotation.
            txn_release_pin();
            for (size_t i = 0; i < m_connections.size(); i++) {
                if (i != conn_id && m_connections[i]->get_connection_state() != conn_disconnected) {
                    m_connections[i]->schedule_fill();
                }
            }
        }
        return true;
    }

    /* Don't exceed requests — but always drain staged monitor commands even after the limit
     * is reached, since those were already counted by the routing side. */
    if (m_config->requests) {
        if (m_key_index_pools[conn_id]->empty() && m_staged_monitor_commands[conn_id].empty() &&
            m_reqs_generated >= m_config->requests) {
            return true;
        }
    }

    /* Cross-shard route-then-stage in-flight backpressure.
     *
     * Several cluster_client routing paths fan work out into *other* shards'
     * queues (m_key_index_pools for built-in SET/GET routing under
     * --read-preference, m_staged_monitor_commands for --monitor-input, the
     * key_index_pool again for cross-shard MGET) without growing the
     * producer's own m_pipeline. fill_pipeline's `m_pipeline->size() < pipeline`
     * gate therefore never throttles the producer: the routing side keeps
     * selecting and pushing while each target drains at most ~pipeline-per-RTT.
     * The target pools grow without bound, the event loop never yields, and
     * `client::finished()` (whose duration counter advances only via
     * response-driven roll_cur_stats() calls) cannot observe --test-time
     * expiry. The benchmark livelocks. R5 round-19's 6479c3b added
     * schedule_fill on the target after each push, but that timer only fires
     * once the producer actually returns to libevent — which it never does
     * while this gate is missing.
     *
     * Couple production to drain by capping the global end-to-end in-flight
     * count — pooled/staged + sent-awaiting-response, which equals
     * (m_reqs_generated - m_reqs_processed) — at pipeline * connection_count,
     * the same total depth a non-staged run would sustain. A connection that
     * still has its own pooled or staged commands to drain is never held
     * here, so draining and therefore forward progress is never blocked;
     * only pure producers pause until responses bring the backlog back under
     * budget. Once a target's response handler bumps m_reqs_processed below
     * the budget, the next fill_pipeline tick on the producer (driven by the
     * target's own schedule_fill chain after responses, or by any other
     * conn's bufferevent callback in the same event loop) sees in_flight <
     * budget and resumes producing. */
    if (m_staged_monitor_commands[conn_id].empty() && m_key_index_pools[conn_id]->empty()) {
        // Clamp the subtraction: m_reqs_generated is normally >= m_reqs_processed,
        // but with --retry-on-error a redirected/replayed request can be processed
        // more than once without a matching generated bump. An unsigned underflow
        // here would wrap to a huge value and wedge this producer permanently, so
        // guard it defensively.
        const unsigned long long in_flight =
            m_reqs_generated > m_reqs_processed ? m_reqs_generated - m_reqs_processed : 0;
        const unsigned long long in_flight_budget =
            (unsigned long long) m_config->pipeline * (unsigned long long) m_connections.size();
        if (in_flight >= in_flight_budget) {
            return true;
        }
    }

    /* In GET-only MGET mode, a connection whose slots own no keys in the
     * configured key range can never generate a request.  Returning true here
     * breaks the fill_pipeline while-loop for that connection so it does not
     * spin consuming CPU.  Other connections (which do have eligible slots)
     * continue to operate normally. */
    if (m_config->multi_key_get > 0 && m_config->ratio.a == 0 && m_config->mget_cache != NULL &&
        m_config->mget_cache->built.load(std::memory_order_acquire) && conn_id < m_mget_conn_slots.size() &&
        m_mget_conn_slots[conn_id].empty() && m_staged_monitor_commands[conn_id].empty()) {
        return true;
    }

    /* Read-preference bootstrap window: if reads under the current
     * --read-preference would otherwise have NO routable target, we would
     * spin in fill_pipeline calling get_key_for_conn -> select_target_conn
     * -> UINT_MAX -> not_available -> retry forever, with the event loop
     * never getting a chance to fire BEV_EVENT_CONNECTED for the in-progress
     * connections. Hold here until at least one read target is live; the
     * bootstrap CLUSTER SLOTS response, plus the next connect callback,
     * will schedule_fill us out of the hold.
     *
     * Strict rp_secondary (with fallback != rpf_primary) only routes to
     * replicas, so the gate triggers as soon as no replica is live. For
     * rp_secondary_preferred and rp_nearest, select_target_conn falls back
     * to the primary, so a no-route situation only arises when *every*
     * connection (primary + replicas) is offline -- a transient state we
     * still want to yield on rather than burn CPU. */
    if (m_config->read_preference != rp_primary) {
        const bool strict_secondary =
            m_config->read_preference == rp_secondary && m_config->read_preference_fallback != rpf_primary;
        // For strict-secondary we need a live REPLICA; for the non-strict
        // modes (which can fall back to the primary) we need *any* live
        // routing target.
        bool any_route = false;
        bool any_replica_warming = false;
        // Walk every shard group so any_replica_warming reflects the whole
        // topology -- select_target_conn is per-slot, so a still-warming
        // replica on ANY shard means bootstrap-window reads to that shard's
        // slots get steered to its primary under the permissive fallback
        // modes. The per-shard scoping attempted in round-41 (e34143f) was
        // unsafe: it let shard A's primary absorb reads while shards B/C
        // were still bootstrapping, leaking ~48/1600 master GETs under
        // secondaryPreferred (round-44 regression rollback).
        for (size_t i = 0; i < m_shard_groups.size(); i++) {
            if (!strict_secondary) {
                shard_connection *p = m_shard_groups[i].primary;
                if (p != NULL && p->is_ready_for_reads()) {
                    any_route = true;
                }
            }
            for (size_t j = 0; j < m_shard_groups[i].replicas.size(); j++) {
                shard_connection *r = m_shard_groups[i].replicas[j];
                if (r == NULL) continue;
                if (r->is_ready_for_reads()) {
                    any_route = true;
                } else if (r->get_connection_state() != conn_disconnected) {
                    // Replica TCP is up (conn_in_progress or conn_connected) but
                    // its setup ladder (AUTH/HELLO/READONLY/CLUSTER SLOTS) has not
                    // completed yet. Treat as "warming"; the next event-loop tick
                    // will progress it to is_ready_for_reads().
                    //
                    // Bounded-wait safety net: this branch only fires while a
                    // replica is in a non-terminal state. If DNS is broken or
                    // every replica is dead, attempt_reconnect() eventually
                    // transitions the conn through disconnect() -> backoff ->
                    // connect() -> ..., and during the backoff window the
                    // state is conn_disconnected, so any_replica_warming flips
                    // false and the gate releases below (primary fallback
                    // works for rp_secondary_preferred / rp_nearest). After
                    // --max-reconnect-attempts the thread is torn down by
                    // event_base_loopbreak. So the hold is never permanent
                    // even with all replicas down. Bugbot round-29 B2.
                    any_replica_warming = true;
                }
            }
        }
        // Bootstrap window for rp_secondary_preferred / rp_nearest: when a
        // replica is still warming (TCP connected but the setup ladder
        // AUTH/HELLO/READONLY/CLUSTER SLOTS hasn't completed) but the primary
        // for that shard is already live, the permissive fallback would
        // silently steer every GET on that shard's slots to the primary until
        // the replica finishes its ladder.
        // tests/test_read_preference_modes.py (test #3 secondary_preferred) and
        // R5 round-25 CI both observed ~50-63 master GETs leak out of 1600
        // total. Hold the producer while ANY configured replica is still
        // warming -- the leak is per-shard and depends on which replicas
        // happen to finish their ladder first, so checking a single
        // "any_live_replica" flag is insufficient (a shard whose replica
        // landed second still leaks GETs to its primary). Once every replica
        // has either completed setup or permanently failed, exit the
        // bootstrap window. The primary-fallback semantics of
        // select_target_conn still apply once we leave this window, so a
        // cluster that genuinely has no replicas is unaffected. The warning
        // ladder below remains the safety net when the bootstrap stretches
        // past 60s.
        //
        // Mixed-workload coverage (Bugbot round-29 B3): the original gate was
        // scoped to ratio.a == 0 (pure GETs), so under --ratio=1:1 GETs
        // could still leak to the primary during warmup. Removing the
        // ratio.a precondition holds the producer for the (short) bootstrap
        // window under mixed traffic too; SETs are deferred along with
        // GETs, which is acceptable cost for the few hundred ms it takes
        // the replicas' ladders to land.
        //
        // Strict-secondary mixed-workload coverage (Cursor[bot] round-32):
        // under --read-preference=secondary the strict_secondary branch
        // above only fires when there is NO route at all (!any_route). In a
        // mixed workload (ratio.b > 0 with ratio.a > 0) the SET path has a
        // route via the primary, so any_route is true and the !any_route
        // gate never trips. GETs then race through get_key_for_conn, which
        // calls client::get_key_for_conn FIRST (advancing the key cursor)
        // and only THEN consults select_target_conn -- which returns
        // UINT_MAX while replicas are still warming. The result is silent
        // key-index loss: every deferred GET burns one key slot before
        // not_available is returned. Extend the warming-gate condition to
        // strict rp_secondary whenever there is any read component, so the
        // producer pauses until at least one replica has finished its
        // setup ladder. Pure-read strict secondary (ratio.b > 0 &&
        // ratio.a == 0) is also covered here, which is strictly more
        // protective than the !any_route gate alone (no behavioural
        // regression -- the producer just yields a few ticks earlier).
        // Primary-fallback exemption (Cursor[bot] round-43): when
        // --read-preference-fallback=primary is set, select_target_conn
        // will gracefully degrade reads to the live primary while replicas
        // are still warming. Blocking the producer here is pure cost --
        // the routing layer already has a valid target, but traffic never
        // reaches it because the gate above pauses every producer until
        // every replica completes its setup ladder. For workloads with
        // long replica warmup (TLS handshakes, large ACLs, slow CLUSTER
        // SLOTS) this caused the benchmark to stall instead of honoring
        // the explicit primary-fallback contract. Skip the gate when
        // fallback=primary; the warning ladder below still trips if EVERY
        // route (primary included) is down.
        if (any_replica_warming && m_config->read_preference_fallback != rpf_primary &&
            (m_config->read_preference == rp_secondary_preferred || m_config->read_preference == rp_nearest ||
             (m_config->read_preference == rp_secondary && m_config->ratio.b > 0))) {
            return true;
        }
        // The all-replicas-unreachable warning + stall hold below is
        // specifically about the read-only spin-loop pathology
        // (fill_pipeline calling select_target_conn -> UINT_MAX ->
        // not_available -> retry forever, with no SETs to keep the
        // pipeline-depth gate happy). Keep the ratio.a==0 precondition for
        // this branch only; mixed workloads have SETs to consume budget so
        // they cannot spin here.
        if (m_config->ratio.a == 0 && !any_route) {
            // Coarse rate-limited operator signal: when ratio.a==0 + no live
            // routing target + non-rp_primary, the benchmark stalls
            // indefinitely (--reconnect-on-error=off can't revive the lost
            // replicas). Emit a single warning at most every 60s so the
            // stall is visible without log spam. Use CLOCK_MONOTONIC so the
            // window is immune to wall-clock jumps.
            struct timespec mono;
            if (clock_gettime(CLOCK_MONOTONIC, &mono) == 0) {
                const long long now_s = (long long) mono.tv_sec;
                if (m_last_no_replica_warning_ts == 0 || now_s - m_last_no_replica_warning_ts >= 60) {
                    const char *rp_str = "primary";
                    switch (m_config->read_preference) {
                    case rp_secondary:
                        rp_str = "secondary";
                        break;
                    case rp_secondary_preferred:
                        rp_str = "secondary-preferred";
                        break;
                    case rp_nearest:
                        rp_str = "nearest";
                        break;
                    default:
                        rp_str = "primary";
                        break;
                    }
                    // Prefix with the worker's pthread id so log readers can
                    // dedupe the N copies emitted per 60s under high thread
                    // counts (one cluster_client per worker thread).
                    benchmark_error_log("warning: [thread %lu] all replicas unreachable under read-only workload "
                                        "(--read-preference=%s); benchmark stalled -- check cluster health or set "
                                        "--reconnect-on-error.\n",
                                        (unsigned long) pthread_self(), rp_str);
                    m_last_no_replica_warning_ts = now_s;
                }
            }
            return true;
        }
    }

    /* Mixed-workload no-route spin guard (self-clearing yield).
     *
     * The read-only bootstrap window above covers ratio.a == 0 (GET-only).
     * For mixed workloads (ratio.a > 0 && ratio.b > 0) writes succeed on the
     * primary, so fill_pipeline's pipeline-depth gate never fires -- but every
     * GET routing attempt returns UINT_MAX (no routable target), causing
     * create_request to return without advancing m_get_ratio_count, so the
     * next fill_pipeline iteration tries the same GET again: a CPU-burning
     * spin until a routing target connects.
     *
     * get_key_for_conn bumps m_strict_no_route_attempts on each read routing
     * failure and resets it on a successful read. Once the counter reaches
     * STRICT_NO_ROUTE_HOLD_THRESHOLD we yield here -- even if `any_route` is
     * true. The destination may be saturated but live; we still need to
     * release the event loop so the queued schedule_fill can fire and the
     * destination can drain. Under pure-MGET + saturated-replica the
     * producer's own pipeline never grows (no SETs), so fill_pipeline would
     * otherwise keep spinning here without ever returning control to
     * libevent.
     *
     * SELF-CLEARING YIELD: the counter is shared per-cluster_client (i.e.,
     * across producer and destination connections within the same worker
     * thread). An unconditional yield would deadlock: the producer trips the
     * threshold and parks (its fill_pipeline breaks, bufferevent_disable),
     * then the destination's queued schedule_fill fires, the destination's
     * fill_pipeline runs, finds counter still >= threshold, yields too, and
     * parks. The counter could then only reset via a successful MGET send
     * (see create_mget_request's reset around line 1582), but reaching that
     * path requires bypassing hold_pipeline -- chicken and egg.
     *
     * Resetting the counter to zero *at the moment we yield* converts the
     * deadlock back to a bounded spin: the next fill_pipeline call sees
     * counter == 0, falls through to create_mget_request, and either sends
     * successfully (counter stays 0) or re-defers (counter slowly rebuilds
     * to STRICT_NO_ROUTE_HOLD_THRESHOLD before the next yield). The worst
     * case is STRICT_NO_ROUTE_HOLD_THRESHOLD defers per yielded iteration --
     * acceptable, and matches the pre-yield bounded-spin behaviour. A
     * "zero-spin" fix would need producer-side wait-on-destination tracking,
     * which is out of scope here.
     *
     * The gate triggers for any non-primary read preference: rp_secondary +
     * non-rpf_primary fallback (replica-only), rp_secondary_preferred and
     * rp_nearest (a UINT_MAX result implies even the primary is offline). */
    if (m_strict_no_route_attempts >= STRICT_NO_ROUTE_HOLD_THRESHOLD && m_config->read_preference != rp_primary) {
        m_strict_no_route_attempts = 0;
        return true;
    }

    /* Write-side / empty-topology spin guard.
     *
     * The gate above only triggers when --read-preference != rp_primary, which
     * leaves the all-malformed CLUSTER SLOTS case (cluster_slots_malformed.bin
     * fuzz fixture, #417) unguarded: after the build-then-swap protection
     * m_shard_groups stays empty, every routing call returns UINT_MAX, and
     * fill_pipeline busy-loops on m_pipeline->size() < pipeline forever (the
     * pipeline never grows because no write is ever issued). The --test-time
     * timer never fires.
     *
     * get_key_for_conn bumps the counter on every no-route (reads AND writes),
     * so by the time it crosses STRICT_NO_ROUTE_HOLD_THRESHOLD here we know
     * routing has been wedged for at least that many attempts. If the topology
     * is empty OR every shard group's primary is unroutable, yield to the
     * event loop so the timer can fire and so any in-progress CLUSTER SLOTS
     * refresh can land. Reset the counter on yield to preserve the
     * self-clearing semantics of the gate above (bounded spin instead of
     * deadlock; see the long comment block on the previous gate for the
     * rationale). */
    if (m_strict_no_route_attempts >= STRICT_NO_ROUTE_HOLD_THRESHOLD) {
        bool any_live_primary = false;
        for (size_t i = 0; i < m_shard_groups.size() && !any_live_primary; i++) {
            shard_connection *p = m_shard_groups[i].primary;
            if (p != NULL && p->get_connection_state() == conn_connected) {
                any_live_primary = true;
            }
        }
        if (m_shard_groups.empty() || !any_live_primary) {
            m_strict_no_route_attempts = 0;
            return true;
        }
    }

    /* In transaction mode the pin connection drives the entire rotation.
     * Non-pin connections must not spin in fill_pipeline; they will be
     * rescheduled via schedule_fill() when the pin is cleared. If the pin
     * connection has disconnected, release it so the remaining connections
     * are not blocked indefinitely. */
    if (m_config->transaction && m_txn_pinned_conn_id != -1 && m_txn_pinned_conn_id != (int) conn_id) {
        if (m_connections[m_txn_pinned_conn_id]->get_connection_state() == conn_disconnected) {
            // Pin dropped; clear it and wake sibling connections so they are
            // not left blocked indefinitely waiting for the disconnected pin.
            txn_release_pin();
            for (size_t i = 0; i < m_connections.size(); i++) {
                if ((unsigned int) i != conn_id && m_connections[i]->get_connection_state() != conn_disconnected) {
                    m_connections[i]->schedule_fill();
                }
            }
        } else {
            return true;
        }
    }

    return false;
}

// Classify a (command_index, kind-of-flow) pair as read vs write for routing.
// The interpretation of command_index differs between the built-in SET/GET
// path and the arbitrary-command path; resolve here so callers don't have to.
bool cluster_client::is_read_command_index(unsigned int command_index) const
{
    if (m_config->arbitrary_commands && m_config->arbitrary_commands->is_defined()) {
        // command_index addresses arbitrary_commands[].
        if (command_index >= m_config->arbitrary_commands->size()) return false;
        const arbitrary_command &cmd = m_config->arbitrary_commands->at(command_index);
        if (cmd.is_read_override == 1) return true;
        if (cmd.is_read_override == 0) return false;
        if (cmd.spec != NULL) return cmd.spec->is_read;
        return false; // unknown command -> safe default: write
    }
    // Built-in path: only SET_CMD_IDX (write) and GET_CMD_IDX (read) are used.
    return command_index == GET_CMD_IDX;
}

get_key_response cluster_client::get_key_for_conn(unsigned int command_index, unsigned int conn_id,
                                                  unsigned long long *key_index)
{
    // first check if we already have a key in the pool
    if (!m_key_index_pools[conn_id]->empty()) {
        *key_index = m_key_index_pools[conn_id]->front();
        m_obj_gen->generate_key(*key_index);

        m_key_index_pools[conn_id]->pop();
        return available_for_conn;
    }

    // generate key
    client::get_key_for_conn(command_index, conn_id, key_index);

    unsigned int hslot = calc_hslot_crc16_cluster(m_obj_gen->get_key(), m_obj_gen->get_key_len());

    // Read-preference-aware target selection. For writes this collapses to
    // the slot's primary owner (identical to the pre-read-pref world). For
    // reads it follows --read-preference. Returns UINT_MAX before bootstrap
    // (no shard_group populated) or under strict rp_secondary when no replica
    // is live -- both cases fall through to "not_available" so the next tick
    // retries or the caller applies a fallback.
    const bool is_read = is_read_command_index(command_index);
    unsigned int target_conn_id = select_target_conn(hslot, is_read);

    if (target_conn_id == UINT_MAX) {
        // Bootstrap / strict-secondary-no-replica path. We do NOT pre-emptively
        // schedule a CLUSTER SLOTS refresh: rp_secondary failing because every
        // replica is down should not whack the topology -- it's a routing
        // gap, not a topology bug. The next event loop tick will retry.
        //
        // Bump the consecutive-failure counter so hold_pipeline can yield the
        // event loop once the threshold is reached. For reads, this prevents a
        // mixed SET/GET workload from busy-spinning fill_pipeline when writes
        // succeed (primary is live) but every GET routing returns UINT_MAX
        // because no replica is available yet.
        //
        // For writes, this counter feeds hold_pipeline's empty-topology spin
        // guard: on an all-malformed CLUSTER SLOTS reply, the build-then-swap
        // protection leaves m_shard_groups empty. Every SET then routes
        // through select_target_conn -> UINT_MAX -> not_available, and
        // without a write-side bump fill_pipeline busy-loops on
        // m_pipeline->size() < pipeline forever (the producer's pipeline
        // never grows, so the pipeline-depth gate never fires).
        // hold_pipeline's empty-topology gate trips on this bump and yields.
        // Saturating at UINT_MAX avoids overflow.
        if (m_strict_no_route_attempts < UINT_MAX) m_strict_no_route_attempts++;
        return not_available;
    }
    // Routing succeeded. Clear the back-off counter only for reads (and only
    // for the producer==target fast path here; the cross-shard branch below
    // defers the reset until after its own not_available gates so a perpetual
    // "routed-but-not-dispatched" loop still trips hold_pipeline's yield).
    //
    // Mixed SET/GET workload rationale: under --read-preference != rp_primary,
    // the counter is a "consecutive read no-route" gauge for hold_pipeline's
    // first gate (the read-only one). Resetting it on a successful write would
    // mask a permanent GET-routing failure (every SET would reset the
    // STRICT_NO_ROUTE_HOLD_THRESHOLD-attempt yield before hold_pipeline could
    // ever trip the read-only gate). The write-side gate in hold_pipeline is
    // keyed on `m_shard_groups.empty() || no live primary`, a state that
    // cannot coexist with a successful write -- so the counter staying high
    // across a successful write does not delay the write-side gate from
    // firing during the all-malformed CLUSTER SLOTS case (the counter will be
    // reset by the gate's own yield path).
    //
    // Cursor bugbot MED (cluster_client.cpp:1083 round-25): the reset used to
    // fire here unconditionally for is_read, BEFORE the cross-shard
    // not_available gates below (setup_done / KEY_INDEX_QUEUE_MAX_SIZE). When
    // a cross-shard read repeatedly hit one of those gates the key index was
    // consumed but no GET was issued AND the counter was reset on each
    // attempt -- hold_pipeline never yielded, fill_pipeline spun. Defer the
    // reset to after the gates so the counter accumulates and the yield
    // gate trips.
    if (target_conn_id == conn_id) {
        if (is_read) {
            m_strict_no_route_attempts = 0;
        }
        benchmark_debug_log("%s generated key=[%.*s] for itself\n", m_connections[conn_id]->get_readable_id(),
                            m_obj_gen->get_key_len(), m_obj_gen->get_key());
        return available_for_conn;
    }

    // handle key for other connection
    unsigned int other_conn_id = target_conn_id;

    // The target connection picked by select_target_conn was live at selection
    // time (conn_is_live_for_routing already filters disconnected nodes), but a
    // TOCTOU window exists between that check and the dispatch here -- the
    // bufferevent can fire BEV_EVENT_EOF in between. If the target is no longer
    // connected at dispatch time, route the recovery based on its role:
    //
    //   PRIMARY disconnect  ->  topology gap. The slot's authoritative owner
    //                           is gone; we cannot route writes (or rp_primary
    //                           reads) anywhere else without a fresh CLUSTER
    //                           SLOTS reply. Schedule a topology refresh on
    //                           the producer connection (this conn_id), which
    //                           was the historical behavior of this guard.
    //
    //   REPLICA disconnect  ->  local routing gap, not a topology change. A
    //                           dead replica must not whack the whole
    //                           cluster's topology view: under skewed
    //                           workloads a single transiently-flapping
    //                           replica would otherwise churn CLUSTER SLOTS
    //                           on every produce tick. Simply defer; on the
    //                           next routing attempt select_target_conn's
    //                           round-robin / nearest / preferred path will
    //                           skip the dead replica and pick a live one
    //                           (or fall back to the primary per
    //                           --read-preference-fallback). The next regular
    //                           topology poll heals naturally.
    //
    // Bugbot finding (cluster_client.cpp:1029): apply the role split so a
    // replica outage doesn't trigger a cluster-wide topology refresh.
    //
    // Round-13: do NOT return not_available here. m_obj_gen->m_next_key
    // has already advanced via client::get_key_for_conn above, so an
    // early return silently drops the key index and the iterator wraps
    // re-writing already-touched keys (the 500000 == 499824 keyspace
    // loss observed in CI). Trigger the topology refresh (primary
    // disconnect only) but fall through to the cross-shard queue push
    // below. Queueing onto the target's pool is safe: the target's
    // fill_pipeline waits for setup_done before draining, and on
    // reconnect the pool drains normally. The read-side setup_done
    // gate at line ~1352 still protects reads that need a populated
    // slot map to route.
    if (m_connections[other_conn_id]->get_connection_state() == conn_disconnected &&
        !m_connections[other_conn_id]->is_replica()) {
        m_connections[conn_id]->set_cluster_slots();
    }

    // Cross-shard write/read: target_conn_id != producer's conn_id (key hashes
    // to a different shard's primary or to a replica). The pre-round-25
    // version gated reads here on the target's cluster_slots_state ==
    // setup_done, mirroring "the slot map must be valid to route." That
    // turned out to share the same key-loss footgun as the writes guard
    // fixed in round-9 (a257fc2): client::get_key_for_conn has already
    // advanced m_obj_gen->m_next_key by the time we reach this gate, so a
    // not_available return silently consumed the key index. The iterator
    // then wrapped at m_key_max and re-wrote already-touched keys -- the
    // same keyspace shortfall round-9 fixed for writes.
    //
    // Cursor bugbot MED (cluster_client.cpp:1129 round-25): drop the
    // setup_done check for reads too. Queueing onto the target's pool is
    // safe regardless of m_cluster_slots: the target's own fill_pipeline
    // calls is_conn_setup_done() (m_authentication && m_db_selection &&
    // m_cluster_slots && m_hello && m_readonly_state all == setup_done)
    // BEFORE draining the pool, so the queued read is held until the
    // ladder completes and the slot map is valid. This mirrors the
    // cross-shard write fix (a257fc2): producer side does not duplicate
    // the readiness check that the drain side already enforces.
    //
    // The READONLY ack concern is already handled by is_conn_setup_done()'s
    // m_readonly_state predicate -- not by m_cluster_slots, which is what
    // this gate was checking.
    //
    // The b73b2a9 cross-shard route-then-stage in-flight backpressure gate
    // in hold_pipeline now bounds the producer side, so we cannot unbounded-
    // push into a stalled target's pool: once in_flight reaches the budget
    // the producer yields and the queued reads drain when the target
    // completes its setup ladder.

    key_index_pool *key_idx_pool = m_key_index_pools[other_conn_id];
    if (key_idx_pool->size() >= KEY_INDEX_QUEUE_MAX_SIZE) {
        // Cursor bugbot MED round-33: queue-cap deferral must bump the
        // strict-no-route counter so hold_pipeline's spin guard
        // (STRICT_NO_ROUTE_HOLD_THRESHOLD) sees it. Without this bump a
        // perpetually-full target pool returns not_available silently and
        // fill_pipeline busy-loops since the pipeline-depth gate never
        // fires (producer's own pipeline does not grow). Mirrors the
        // empty-topology bump at line 1595. Saturating at UINT_MAX avoids
        // overflow.
        if (m_strict_no_route_attempts < UINT_MAX) m_strict_no_route_attempts++;
        return not_available;
    }

    // Cross-shard routing actually committed: reset the back-off counter
    // now (deferred from the producer==target reset above). Cursor bugbot
    // MED round-25: the prior placement reset BEFORE the gates above, so
    // a perpetual not_available from the setup_done or queue-cap gate
    // stayed invisible to hold_pipeline's yield. Resetting here means the
    // counter only clears on an actually-queued read, mirroring the
    // semantics of the producer==target reset. Round-33: queue-cap path
    // now also bumps the counter above, so hold_pipeline yields.
    if (is_read) {
        m_strict_no_route_attempts = 0;
    }

    // store command and key for the other connection
    benchmark_debug_log("%s generated key=[%.*s] for %s\n", m_connections[conn_id]->get_readable_id(),
                        m_obj_gen->get_key_len(), m_obj_gen->get_key(),
                        m_connections[other_conn_id]->get_readable_id());

    key_idx_pool->push(command_index);
    key_idx_pool->push(*key_index);
    // Wake the target's bufferevent so its own fill_pipeline drains the pool.
    // Without this, cross-shard reads/writes accumulate in the target's
    // key_index_pool but the target stays idle (its bufferevent has no
    // pending I/O). Under --test-time mode client::finished() advances only
    // on completed ops, so the benchmark livelocks. Mirrors the wake-up in
    // create_monitor_request_cluster's staged-command path.
    m_connections[other_conn_id]->schedule_fill();
    return available_for_other_conn;
}

bool cluster_client::create_arbitrary_request(unsigned int command_index, struct timeval &timestamp,
                                              unsigned int conn_id)
{
    /* In arbitrary request, where we send the command arg by arg, we need to check for a key command,
     * if the generated key belongs to this connection before starting to send it */
    assert(m_key_index_pools[conn_id]->empty());

    const arbitrary_command &cmd = get_arbitrary_command(command_index);

    /* --monitor-input in cluster mode: select the command, parse its first key, and route
     * to the shard that owns that slot. Commands for other shards are staged and that shard
     * is woken up via schedule_fill(); this connection returns immediately without sending. */
    if (cmd.command_args.size() == 1 && cmd.command_args[0].type == monitor_random_type) {
        return create_monitor_request_cluster(command_index, timestamp, conn_id);
    }

    /* --transaction: one full rotation of --command entries = one transactional
     * unit (e.g. WATCH/MULTI/.../EXEC). Pin every command in the rotation to a
     * single shard connection so keyless commands stay on the same connection
     * as the keyed ones. The pin is set on the first command of a rotation and
     * cleared when the rotation wraps back to index 0 (detected here via index
     * 0 + ratio counter 0). */
    if (m_config->transaction) {
        // The cluster-mode startup guard at memtier_benchmark.cpp rejects
        // arbitrary commands with keys_count > 1, so the single-key pool
        // layout below is sufficient. If cluster mode ever grows multi-key
        // arbitrary command support, the pool push has to loop over every
        // key_type arg.
        assert(cmd.keys_count <= 1);

        if (m_arbitrary_command_rotation_seq != m_txn_observed_rotation_seq) {
            m_txn_observed_rotation_seq = m_arbitrary_command_rotation_seq;
            m_txn_pinned_conn_id = -1;
            m_txn_has_staged_key = false;
            m_txn_pin_lost_warned = false;
            /* Wake up connections that were held back by hold_pipeline so
             * they can participate in the new rotation's lookahead. */
            for (size_t i = 0; i < m_connections.size(); i++) {
                if ((unsigned int) i != conn_id && m_connections[i]->get_connection_state() != conn_disconnected) {
                    m_connections[i]->schedule_fill();
                }
            }
        }

        if (m_txn_pinned_conn_id == -1) {
            /* Pin the rotation to the shard that owns the first KEYED
             * command in this rotation. A rotation that starts with one or
             * more keyless commands (e.g. MULTI before SET) must still be
             * routed to the slot of the upcoming keyed command, otherwise
             * the keyless commands land on an arbitrary shard and the
             * keyed commands then MOVED-back to the right one — breaking
             * transaction state. If the rotation has no keyed commands at
             * all, fall back to the current connection.
             *
             * Critically, the key the lookahead generates is *not* thrown
             * away. obj_gen->get_key_index() advances the per-iter
             * sequential counter (and similarly for Zipfian/etc.), so
             * discarding it would burn an extra key per rotation —
             * halving the effective key range under --key-pattern=S.
             * Instead we stash the key_index in m_txn_staged_key_index,
             * and the actual send for that command_index pushes it onto
             * the pool just before calling client::create_arbitrary_request. */
            int target_conn = -1;
            unsigned long long staged_key_index = 0;
            bool have_staged_key = false;
            m_txn_has_staged_key = false;
            unsigned int total = m_config->arbitrary_commands->size();
            for (unsigned int off = 0; off < total; off++) {
                unsigned int look_idx = (m_executed_command_index + off) % total;
                const arbitrary_command &look = get_arbitrary_command(look_idx);
                if (look.stats_only) continue;
                if (look.keys_count == 0) continue;

                client::get_key_for_conn(look_idx, conn_id, &staged_key_index);
                have_staged_key = true;

                /* Reconstruct the actual key string that would be sent on
                 * the wire — data_prefix + obj_gen.get_key() + data_suffix
                 * — and hash it the way Redis does (honoring {tag}
                 * substrings). The default memtier slot computation only
                 * sees the generated portion of the key, so it would
                 * route to a random shard when the keyed argument carries
                 * hash-tag affixes. */
                const command_arg *key_arg = NULL;
                for (size_t a = 0; a < look.command_args.size(); a++) {
                    if (look.command_args[a].type == key_type) {
                        key_arg = &look.command_args[a];
                        break;
                    }
                }
                const char *gen_key = m_obj_gen->get_key();
                unsigned int gen_key_len = m_obj_gen->get_key_len();
                unsigned int hslot;
                if (key_arg != NULL && key_arg->has_key_affixes) {
                    std::string full;
                    full.reserve(key_arg->data_prefix.size() + gen_key_len + key_arg->data_suffix.size());
                    full.append(key_arg->data_prefix);
                    full.append(gen_key, gen_key_len);
                    full.append(key_arg->data_suffix);
                    hslot = calc_hslot_crc16_with_hash_tag(full.data(), full.size());
                } else {
                    hslot = calc_hslot_crc16_with_hash_tag(gen_key, gen_key_len);
                }
                {
                    // Resolve slot via the shard_group indirection. If the
                    // map isn't populated yet (slot_primary_conn_id returns
                    // UINT_MAX), leave target_conn at -1 so the fallback
                    // below routes to the current conn_id; matches the prior
                    // behavior where m_slot_to_shard was zero-initialized.
                    unsigned int pcid = slot_primary_conn_id(hslot);
                    target_conn = (pcid == UINT_MAX) ? -1 : (int) pcid;
                }
                break;
            }
            /* If we couldn't find a keyed cmd, or the slot mapping isn't
             * populated yet (target_conn out of range), or the elected shard
             * is currently disconnected (e.g. pin just dropped and hasn't
             * reconnected yet), fall back to the current conn so the rotation
             * can proceed rather than re-pinning to a dead connection and
             * permanently stalling all other conns via hold_pipeline. */
            if (target_conn < 0 || target_conn >= (int) m_connections.size() ||
                m_connections[target_conn]->get_connection_state() == conn_disconnected) {
                target_conn = (int) conn_id;
                // keep have_staged_key: the key was already generated and the
                // sequential counter advanced, so we must reuse it here (on
                // the fallback conn_id pool) rather than discarding it and
                // leaving a gap in the sequential key range.
            }
            m_txn_pinned_conn_id = target_conn;
            if (have_staged_key) {
                m_txn_staged_key_index = staged_key_index;
                m_txn_has_staged_key = true;
            }
        }

        /* Only the pinned connection drives the rotation. Non-pin conns
         * return false so they don't advance m_executed_command_index or
         * generate a request. The event loop will call the pin connection
         * itself, which sends the command in order. Returning false here
         * (rather than queueing onto the pin's pool) keeps the pin's
         * pipeline depth honest — otherwise late-rotation MULTI/SET
         * fragments would pile up in the pool and get dropped at
         * shutdown, silently losing committed-side data.
         *
         * If the pin was just assigned to a different connection, wake it
         * up explicitly. Without this, a pin whose bufferevent was silenced
         * by the idle path (e.g. it was a non-pin held by hold_pipeline)
         * would never call fill_pipeline again and the benchmark deadlocks. */
        if ((unsigned int) m_txn_pinned_conn_id != conn_id) {
            m_connections[m_txn_pinned_conn_id]->schedule_fill();
            return false;
        }

        /* For keyed commands the lookahead may have pre-generated a key
         * stored in m_txn_staged_key_index. Use it so the per-iter key
         * counter advances exactly once per rotation. */
        if (cmd.keys_count > 0) {
            if (m_txn_has_staged_key) {
                m_key_index_pools[conn_id]->push(m_txn_staged_key_index);
                m_txn_has_staged_key = false;
            } else if (m_key_index_pools[conn_id]->empty()) {
                unsigned long long key_index;
                client::get_key_for_conn(command_index, conn_id, &key_index);
                m_key_index_pools[conn_id]->push(key_index);
            }
        }
        client::create_arbitrary_request(command_index, timestamp, conn_id);
        return true;
    }

    /* keyless command: no slot to hash, but if the command is classified as a
     * read and read_preference != primary, steer it toward a replica so that
     * e.g. "DBSIZE --command-is-read" or "PING --command-is-read" honours the
     * same preference as keyed reads.  We use slot 0 as an arbitrary anchor —
     * the slot itself is irrelevant for a keyless command; we only need it to
     * resolve a shard_group and let select_target_conn apply the policy.
     * If the topology is not ready yet (UINT_MAX) fall back to conn_id.
     *
     * Cursor bugbot MED (deferred): the slot-0 anchor pins keyless arbitrary
     * reads to shard_group[0]'s replicas under rp_secondary, so other shards'
     * live replicas are ignored. The right fix is to round-robin a synthetic
     * slot across populated shard groups (or to add a select_replica_for_any_shard
     * helper that returns the next live replica regardless of slot), but that
     * touches the shared rp_secondary / rp_nearest / rp_secondary_preferred
     * fallback ladder and is out of scope for the read-preference rollout.
     * Tracked in #457. */
    if (cmd.keys_count == 0) {
        unsigned int send_conn_id = conn_id;
        const bool is_read = is_read_command_index(command_index);
        if (is_read && m_config->read_preference != rp_primary) {
            unsigned int routed = select_target_conn(0 /* any slot */, true);
            if (routed == UINT_MAX) {
                /* No eligible target right now (e.g. strict rp_secondary with
                 * rpf_error/queue and every replica offline). Signal defer to
                 * the caller -- returning false here keeps create_request
                 * from advancing m_executed_command_index, so the batch is
                 * retried on the next fill_pipeline tick rather than silently
                 * routed to the primary and the read-preference contract
                 * violated. Mirrors the built-in MGET m_mget_defer path. */
                if (m_strict_no_route_attempts < UINT_MAX) m_strict_no_route_attempts++;
                return false;
            }
            if (routed < m_connections.size()) {
                send_conn_id = routed;
            }
        }
        /* Cross-connection backpressure: when the keyless arbitrary read is
         * routed to a different connection than the producer (replica under
         * non-primary read_preference), fill_pipeline's
         * `m_pipeline->size() < pipeline` gate caps only the producer
         * (conn_id). The destination's actual in-flight depth is invisible
         * here, so without this guard the producer keeps issuing reads and
         * the destination's pipeline grows past --pipeline. Mirror the
         * create_mget_request cross-shard cap at line ~2089: defer when the
         * destination is at its per-connection pipeline cap, bump the
         * strict-no-route counter (saturating) so hold_pipeline can trip
         * the yield gate, and wake the destination so its fill_pipeline can
         * drain. */
        if (send_conn_id != conn_id &&
            (unsigned int) m_connections[send_conn_id]->get_pending_resp() >= m_config->pipeline) {
            m_connections[send_conn_id]->schedule_fill();
            if (m_strict_no_route_attempts < UINT_MAX) m_strict_no_route_attempts++;
            return false;
        }
        client::create_arbitrary_request(command_index, timestamp, send_conn_id);
        if (is_read) {
            m_strict_no_route_attempts = 0;
            record_read_routing(command_index, m_connections[send_conn_id]->is_replica());
        }
        return true;
    }

    /* Normal key placeholder handling */
    unsigned long long key_index;
    get_key_response res = get_key_for_conn(command_index, conn_id, &key_index);

    if (res == not_available) return false;

    /* If we generated a key for a different connection, we will use it later */
    if (res == available_for_other_conn) return true;

    /* We got a key for this connection, put it back into the pool and
     * use it inside client::create_arbitrary_request() */
    m_key_index_pools[conn_id]->push(key_index);
    client::create_arbitrary_request(command_index, timestamp, conn_id);

    // Read-routing observability: when routing chose conn_id (available_for_conn),
    // the send just happened on this connection. Bump the per-command counter
    // so it matches the conn's role.
    if (is_read_command_index(command_index)) {
        record_read_routing(command_index, m_connections[conn_id]->is_replica());
    }

    return true;
}

bool cluster_client::create_get_request(struct timeval &timestamp, unsigned int conn_id)
{
    // Snapshot the connection's pending-resp counter; push_req() on the
    // shard_connection bumps it for every user-level request we send. If
    // the base implementation sent a GET on THIS connection, pending_resp
    // increments by 1; if it routed the work to another connection's pool
    // (or returned not_available) pending_resp stays put. That distinction
    // is what we need to attribute the read to the correct (primary vs
    // replica) endpoint here, without re-implementing the routing logic.
    const int before = m_connections[conn_id]->get_pending_resp();
    bool ok = client::create_get_request(timestamp, conn_id);
    if (!ok) return false;
    const int after = m_connections[conn_id]->get_pending_resp();
    if (after > before) {
        // Deferred follow-up (#101): this counter records the attempted-routing
        // decision at send time. A subsequent -READONLY rejection (handled in
        // handle_response around line ~2140) triggers a retry onto the
        // primary, but the retry does NOT re-bump the counter. The result is
        // that Ops from Replica is mildly overstated and Ops from Primary
        // mildly understated in the presence of -READONLY retries. The skew
        // is bounded by the -READONLY rate, which is normally zero (only fires
        // on misclassification or topology drift). A clean fix would
        // decrement-on-retry here and re-record at successful retry-send time;
        // not done yet because the simpler attempted-routing semantics are
        // adequate for the typical zero-READONLY workload.
        record_builtin_read_routing(rt_get, m_connections[conn_id]->is_replica());
    }
    return ok;
}

bool cluster_client::create_mget_request(struct timeval &timestamp, unsigned int conn_id)
{
    // Reset the defer flag at entry so stale true from a prior defer call is
    // never mistaken for a defer when we actually hit an exhausted path below.
    m_mget_defer = false;

    // Only reached when --multi-key-get is set.
    // Use the pre-built slot cache so all N keys in this MGET share one hash
    // slot — Redis requires exact same-slot (not just same-node) for MGET in
    // cluster mode. Cache is rebuilt on every topology change via
    // build_mget_slot_cache() at the end of handle_cluster_slots().
    unsigned int keys_count = m_config->ratio.b - m_get_ratio_count;
    if ((int) keys_count > m_config->multi_key_get) keys_count = m_config->multi_key_get;
    if (keys_count == 0) return false;

    if (conn_id >= m_mget_conn_slots.size() || m_mget_conn_slots[conn_id].empty()) {
        // Cache not ready, no key in the configured range maps to this shard,
        // or this connection is a replica (slot ownership is primary-only;
        // see build_mget_slot_cache). The "exhausted" semantics here — m_mget_defer
        // stays false so the caller force-bumps m_get_ratio_count to the GET cap
        // and resets the SET/GET cycle on the next tick — are exactly what keeps
        // mixed --ratio + --multi-key-get workloads progressing on replica
        // fill_pipeline ticks: the replica's SET phase still produces routed SETs
        // (which select_target_conn dispatches to the primary), and the would-be
        // replica MGET is correctly elided because the primary's own
        // fill_pipeline is the sole producer (select_target_conn there honors
        // --read-preference and may dispatch the actual MGET back to a replica).
        return false;
    }

    // Round-robin over the slots owned by this connection. Compute the
    // target slot via the cursor BUT do not advance the cursor yet. Both
    // defer paths below (no-route, pipeline-cap) must leave the cursor
    // untouched so the deferred slot gets retried on the next tick — a
    // pre-bumped cursor silently dropped deferred slots on the floor (the
    // cursor advanced at the tick rate, not the send rate).
    size_t &sc = m_mget_conn_slot_cursor[conn_id];
    unsigned int target_slot = m_mget_conn_slots[conn_id][sc % m_mget_conn_slots[conn_id].size()];

    // Read-preference routing for MGET. The slot cache is built off the
    // primary's conn_id, so by default conn_id IS the slot's primary. When
    // --read-preference != primary we redirect this MGET to the configured
    // node class. If routing returns a different conn, hand it the work via
    // a tiny per-conn outbound queue (see m_pending_mget_send below).
    const unsigned int routed = select_target_conn(target_slot, true /* MGET is always a read */);
    if (routed == UINT_MAX) {
        // Strict-secondary with no live replica and rpf_error/queue. The
        // routing policy has no eligible replica right now, but may have one
        // on the next topology refresh or fill_pipeline tick. Signal defer so
        // the caller does NOT advance m_get_ratio_count; the batch will be
        // retried rather than silently abandoned.
        //
        // Bump the strict-no-route counter so hold_pipeline's second gate
        // (STRICT_NO_ROUTE_HOLD_THRESHOLD) trips and parks the producer
        // instead of busy-spinning in mixed SET+MGET workloads (where the
        // writes keep landing on the primary, fill_pipeline's pipeline-depth
        // gate never fires, and no other read-failure site increments this
        // counter). Mirrors the keyless-arbitrary defer path at
        // cluster_client.cpp:1393.
        if (m_strict_no_route_attempts < UINT_MAX) m_strict_no_route_attempts++;
        m_mget_defer = true;
        return false;
    }

    // Cross-connection backpressure: when MGET is routed to a different
    // connection than the producer (replica / nearest-mode / preferred
    // fallback), fill_pipeline's `m_pipeline->size() < pipeline` gate caps
    // only the producer (conn_id). The destination's actual in-flight depth
    // is invisible at this site, so without this guard the producer keeps
    // issuing MGETs and the destination's pipeline grows past --pipeline ->
    // the replica is overloaded and latency tail balloons. Mirror the
    // monitor-input route-then-stage backpressure pattern (m_reqs_generated
    // - m_reqs_processed in_flight clamp at hold_pipeline, plus the staged
    // queue cap at create_monitor_request_cluster:1447) by deferring the
    // MGET when the destination is at its per-connection pipeline cap.
    // schedule_fill() wakes the destination so its own fill_pipeline can
    // drain and re-check; the next outer create_request tick rebalances.
    if (routed != conn_id && (unsigned int) m_connections[routed]->get_pending_resp() >= m_config->pipeline) {
        m_connections[routed]->schedule_fill();
        // Bump the strict-no-route counter on pipeline-cap defer too. In
        // pure-MGET workloads (--ratio 0:N --multi-key-get) with a
        // saturated destination replica the producer's pipeline never
        // grows (no writes land here), so fill_pipeline's pipeline-depth
        // gate never fires and the while-condition stays true. Without
        // bumping the counter here, hold_pipeline cannot trip the
        // STRICT_NO_ROUTE_HOLD_THRESHOLD yield gate, and schedule_fill on
        // the destination is a zero-timer that requires libevent control
        // — control the tight in-call spin would never yield to. Bumping
        // the counter mirrors the no-route arm above.
        if (m_strict_no_route_attempts < UINT_MAX) m_strict_no_route_attempts++;
        m_mget_defer = true;
        return false;
    }

    // Both defer guards passed: commit the cursor advance so the next
    // fill_pipeline tick picks the next slot in round-robin.
    sc++;

    std::vector<unsigned long long> &slot_keys = m_config->mget_cache->slot_keys[target_slot];
    size_t &kc = m_mget_slot_cursor[target_slot];

    m_keylist->clear();
    for (unsigned int i = 0; i < keys_count; i++) {
        unsigned long long idx = slot_keys[kc % slot_keys.size()];
        kc++;
        m_obj_gen->generate_key(idx);
        m_keylist->add_key(m_obj_gen->get_key(), m_obj_gen->get_key_len());
    }

    m_connections[routed]->send_mget_command(&timestamp, m_keylist);
    // routed_ops is bumped by shard_connection::push_req. Record the
    // read-routing decision (Ops from Primary / Ops from Replica) here
    // because the send-side has no other context about the routing class.
    record_builtin_read_routing(rt_get, m_connections[routed]->is_replica());
    // Successful read route: reset strict-no-route counter so the spin
    // guard doesn't trip on healthy pure-MGET workloads with intermittent
    // defers. Mirrors get_key_for_conn's reset at line 1148.
    m_strict_no_route_attempts = 0;
    return true;
}

void cluster_client::create_request(struct timeval timestamp, unsigned int conn_id)
{
    /* Drain staged monitor commands that were routed here from another shard connection. */
    if (!m_staged_monitor_commands[conn_id].empty()) {
        process_staged_monitor_command(timestamp, conn_id);
        return;
    }

    /* If pool is empty continue with base class */
    if (m_key_index_pools[conn_id]->empty()) {
        client::create_request(timestamp, conn_id);
        return;
    }

    unsigned int pool_size = m_key_index_pools[conn_id]->size();
    unsigned int command_index = m_key_index_pools[conn_id]->front();
    m_key_index_pools[conn_id]->pop();

    if (m_config->arbitrary_commands->is_defined()) {
        const int pre_pending = m_connections[conn_id]->get_pending_resp();
        client::create_arbitrary_request(command_index, timestamp, conn_id);
        // Per-arbitrary-command read-routing counters. We only bump when the
        // send actually landed on conn_id (pending-resp grew). If routing
        // dispatched the key to another conn's pool, the bump fires when
        // that conn drains the pool through this same path.
        if (is_read_command_index(command_index) && m_connections[conn_id]->get_pending_resp() > pre_pending) {
            record_read_routing(command_index, m_connections[conn_id]->is_replica());
        }
    } else if (command_index == SET_CMD_IDX) {
        create_set_request(timestamp, conn_id);
    } else if (command_index == GET_CMD_IDX) {
        // create_get_request is virtual; the cluster override records the
        // built-in read-routing counter so we don't bump it twice here.
        create_get_request(timestamp, conn_id);
    } else {
        assert("Unexpected command index");
    }

    /* Make sure we used pair of command and key index */
    assert(m_key_index_pools[conn_id]->size() == pool_size - 2);
}

// Send a staged monitor command that was pre-routed to this shard by another connection.
// parsed_cmd was already split at staging time; we only need to format (RESP-frame) and send.
void cluster_client::process_staged_monitor_command(struct timeval /*timestamp*/, unsigned int conn_id)
{
    staged_monitor_cmd staged = std::move(m_staged_monitor_commands[conn_id].front());
    m_staged_monitor_commands[conn_id].pop();

    // format_arbitrary_command mutates arg->data in-place; call it here at drain time
    // (not at staging time) so the RESP framing is applied on the correct connection's protocol.
    if (!m_connections[conn_id]->get_protocol()->format_arbitrary_command(staged.parsed_cmd)) {
        benchmark_error_log("warning: skipping unformattable staged monitor command at line %zu\n", staged.source_line);
        // m_reqs_generated was incremented when this command was staged. Undo it now so
        // a --requests run doesn't hang waiting for a response that will never arrive.
        m_reqs_generated--;
        return;
    }

    int cmd_size = 0;
    for (unsigned int i = 0; i < staged.parsed_cmd.command_args.size(); i++) {
        const command_arg *arg = &staged.parsed_cmd.command_args[i];
        if (arg->type == const_type) {
            cmd_size += m_connections[conn_id]->send_arbitrary_command(arg);
        }
    }
    if (cmd_size == 0) {
        // format_arbitrary_command succeeded but produced no sendable bytes — guard against
        // pushing a zero-length phantom request that the server would never respond to.
        m_reqs_generated--;
        return;
    }
    // Use the enqueue timestamp so latency reflects selection→response, not drain→response.
    m_connections[conn_id]->send_arbitrary_command_end(staged.stats_index, &staged.enqueue_time, cmd_size);
}

// Select a monitor command, extract its first key to compute the target shard slot, and either
// send it here (slot belongs to this connection) or stage it for the owning shard connection.
bool cluster_client::create_monitor_request_cluster(unsigned int command_index, struct timeval &timestamp,
                                                    unsigned int conn_id)
{
    // Select the command from the monitor file.
    size_t selected_index = 0;
    std::string raw_cmd;
    if (m_config->monitor_pattern == 'R') {
        raw_cmd = m_config->monitor_commands->get_random_command(m_obj_gen, &selected_index);
    } else {
        raw_cmd = m_config->monitor_commands->get_next_sequential_command(&selected_index);
    }
    size_t stats_index = m_config->monitor_commands->get_stats_index(selected_index);

    // Parse the raw command so we can read the first key *before* format_arbitrary_command
    // rewrites arg->data with RESP framing.
    arbitrary_command temp_cmd(raw_cmd.c_str());
    if (!temp_cmd.split_command_to_args()) {
        benchmark_error_log("warning: skipping malformed monitor command at line %zu: %s\n", selected_index + 1,
                            raw_cmd.c_str());
        // Return false so m_reqs_generated is not incremented — no request was sent
        // and no response will arrive, so incrementing would cause a --requests hang.
        return false;
    }

    // Determine target shard from the first key argument (index 1 = first arg after command name).
    // Fall back to the current connection if topology isn't ready yet or there is no key.
    //
    // Cursor bugbot MED (deferred): --monitor-input cluster replay always
    // targets slot-primary owners, ignoring --read-preference. MONITOR is
    // intrinsically per-shard streaming and doesn't map cleanly to
    // MongoDB-style read preferences (a "secondary" preference on a stream
    // workload changes the captured traffic class, not just where reads land).
    // Reworking this requires per-stream replica selection and replica-side
    // ordering guarantees we don't have today. Tracked in #457.
    unsigned int target_conn = conn_id;
    if (temp_cmd.command_args.size() >= 2) {
        const std::string &key = temp_cmd.command_args[1].data;
        if (!key.empty()) {
            uint32_t slot = calc_hslot_crc16_with_hash_tag(key.c_str(), key.size());
            // Same primaries-only resolution as the prior m_slot_to_shard
            // read; the shard_group indirection is invisible at this site.
            unsigned int shard = slot_primary_conn_id(slot);
            if (shard != UINT_MAX && shard < m_connections.size() &&
                m_connections[shard]->get_connection_state() != conn_disconnected &&
                m_connections[shard]->get_cluster_slots_state() == setup_done) {
                target_conn = shard;
            }
        }
    }

    if (target_conn != conn_id) {
        // Stage the pre-selected, pre-split command for the owning shard and wake it up.
        // Storing the already-split arbitrary_command avoids re-parsing at drain time.
        // format_arbitrary_command is intentionally deferred to drain: it mutates arg->data
        // in-place with RESP framing and must be called per-connection at send time.
        // Cap the queue to prevent unbounded memory growth under skewed workloads.
        if (m_staged_monitor_commands[target_conn].size() < STAGED_MONITOR_QUEUE_MAX_SIZE) {
            staged_monitor_cmd staged{std::move(temp_cmd), stats_index, timestamp, selected_index + 1};
            m_staged_monitor_commands[target_conn].push(std::move(staged));
            m_connections[target_conn]->schedule_fill();
            return true;
        }
        // Staged queue is full: fall through and send directly on conn_id.
        // Redis will issue -MOVED; handle_moved() refreshes topology so future
        // commands route correctly. Sending here is safe and avoids a --requests hang
        // that would result from silently dropping a counted request.
        benchmark_debug_log("staged monitor queue for conn %u full, sending directly on conn %u (expect MOVED)\n",
                            target_conn, conn_id);
    }

    // The slot belongs to this connection (or queue-full fallback) — format and send inline.
    if (!m_connections[conn_id]->get_protocol()->format_arbitrary_command(temp_cmd)) {
        benchmark_error_log("warning: skipping unformattable monitor command at line %zu\n", selected_index + 1);
        return false;
    }

    int cmd_size = 0;
    for (unsigned int i = 0; i < temp_cmd.command_args.size(); i++) {
        const command_arg *arg = &temp_cmd.command_args[i];
        if (arg->type == const_type) {
            cmd_size += m_connections[conn_id]->send_arbitrary_command(arg);
        }
    }
    if (cmd_size == 0) {
        // Defensive: formatted command produced no sendable bytes; nothing was written
        // to the socket so no response will arrive. Don't count as a generated request.
        return false;
    }
    m_connections[conn_id]->send_arbitrary_command_end(stats_index, &timestamp, cmd_size);
    return true;
}

// In case of -MOVED response, we sends CLUSTER SLOTS command to get the new topology
void cluster_client::handle_moved(unsigned int conn_id, struct timeval timestamp, request *request,
                                  protocol_response *response)
{
    // update stats
    if (request->m_type == rt_get) {
        m_stats.update_moved_get_op(&timestamp, response->get_total_len(), request->m_size,
                                    ts_diff(request->m_sent_time, timestamp));
    } else if (request->m_type == rt_set) {
        m_stats.update_moved_set_op(&timestamp, response->get_total_len(), request->m_size,
                                    ts_diff(request->m_sent_time, timestamp));
    } else if (request->m_type == rt_arbitrary) {
        arbitrary_request *ar = static_cast<arbitrary_request *>(request);
        m_stats.update_moved_arbitrary_op(&timestamp, response->get_total_len(), request->m_size,
                                          ts_diff(request->m_sent_time, timestamp), ar->index);
    } else {
        assert(0);
    }

    // connection already issued 'cluster slots' command, wait for slots mapping to be updated
    if (m_connections[conn_id]->get_cluster_slots_state() != setup_done) return;

    // flush stale routing entries for this connection's old slot ownership
    // Cross-shard reads that were routed to this connection (via
    // create_request_for_other) push a (command_index, key_index) pair into
    // m_key_index_pools[conn_id] AFTER client::create_request has already
    // incremented m_reqs_generated (client.cpp:656 for GET, the arbitrary
    // path at create_request_for_other for --command). Clearing the pool here
    // without compensating leaves m_reqs_processed < m_reqs_generated
    // permanently, so a --requests run hangs and a --test-time run
    // mis-accounts pending in-flight. Compensate by n/2 (pairs). Defensive
    // clamp guards against underflow.  Matches the pattern at
    // connect_shard_connection.
    key_index_pool empty_queue;
    std::swap(*m_key_index_pools[conn_id], empty_queue);
    {
        const size_t n = empty_queue.size() / 2;
        m_reqs_generated -= (m_reqs_generated >= n) ? n : m_reqs_generated;
    }
    {
        std::queue<staged_monitor_cmd> empty_staged;
        std::swap(m_staged_monitor_commands[conn_id], empty_staged);
        // Staged commands were already counted in m_reqs_generated at staging time.
        // Compensate so a --requests run does not hang waiting for phantom responses.
        // Defensive clamp: entries in empty_staged are popped before m_reqs_generated
        // is decremented so underflow cannot occur today, but guard explicitly so a
        // future refactor does not silently wrap to 2^64.  Matches the pattern at
        // hold_pipeline.
        {
            const size_t n = empty_staged.size();
            m_reqs_generated -= (m_reqs_generated >= n) ? n : m_reqs_generated;
        }
    }

    // set connection to send 'CLUSTER SLOTS' command
    m_connections[conn_id]->set_cluster_slots();
}

// In case of -ASK response, we ignore the response and we will update to the new topology when we get -MOVED response
void cluster_client::handle_ask(unsigned int conn_id, struct timeval timestamp, request *request,
                                protocol_response *response)
{
    // update stats
    if (request->m_type == rt_get) {
        m_stats.update_ask_get_op(&timestamp, response->get_total_len(), request->m_size,
                                  ts_diff(request->m_sent_time, timestamp));
    } else if (request->m_type == rt_set) {
        m_stats.update_ask_set_op(&timestamp, response->get_total_len(), request->m_size,
                                  ts_diff(request->m_sent_time, timestamp));
    } else if (request->m_type == rt_arbitrary) {
        arbitrary_request *ar = static_cast<arbitrary_request *>(request);
        m_stats.update_ask_arbitrary_op(&timestamp, response->get_total_len(), request->m_size,
                                        ts_diff(request->m_sent_time, timestamp), ar->index);
    } else {
        assert(0);
    }
}

// Try to resend the request to the connection that now owns the key's slot.
// Falls back to retrying on the same connection if the key is not captured
// (e.g. arbitrary command without --retry-on-error key plumbing). Returns true
// if ownership of `req` was transferred to a retry queue.
//
// Read-preference-aware: for read-class requests, select_target_conn honors
// --read-preference on retry, so a read that hit MOVED on a replica that no
// longer owns the slot can be re-routed to a *different* replica (or the
// primary, depending on mode). Write-class retries always go to the primary.
// The exception is `-READONLY` (handled by caller above): we force routing
// to the primary regardless of read_preference, because the server has
// explicitly told us the replica won't accept this command class.
bool cluster_client::retry_after_redirect(unsigned int conn_id, request *req)
{
    if (!m_config->retry_on_error) return false;
    if (!req || !req->m_serialized || req->m_serialized_len == 0) return false;

    unsigned int target = conn_id;
    if (req->m_key && req->m_key_len > 0) {
        // Use the hash-tag-aware variant so a retry of an arbitrary command
        // whose key carries a {tag} prefix (e.g. "{foo}-key-5") routes to
        // the correct shard. calc_hslot_crc16_cluster hashes the full string
        // and would map to a different slot, causing an infinite MOVED loop.
        unsigned int hslot = calc_hslot_crc16_with_hash_tag(req->m_key, req->m_key_len);
        const bool is_read = classify_read(req);
        unsigned int mapped = select_target_conn(hslot, is_read);
        // select_target_conn returns UINT_MAX when strict rp_secondary finds no
        // live replica AND fallback != rpf_primary. In that case we must NOT
        // silently fall back to the primary — that would violate the "replicas
        // only" contract. Return false so the caller calls
        // finalize_dropped_redirect, consistent with the original create path
        // returning not_available and --read-preference-fallback=error/queue
        // semantics.
        // Non-strict modes (rp_secondary_preferred, rp_nearest, rp_primary, and
        // rp_secondary+rpf_primary) already fold the primary into the returned
        // conn_id inside select_target_conn, so UINT_MAX here can only mean
        // "all nodes are offline" — the fall-through to slot_primary_conn_id
        // below is the right recovery for that transient topology gap.
        if (mapped == UINT_MAX && is_read && m_config->read_preference == rp_secondary &&
            m_config->read_preference_fallback != rpf_primary) {
            return false;
        }
        // For any other UINT_MAX (offline topology / non-strict mode), try the
        // slot primary as a best-effort recovery so the retry doesn't get stuck.
        if (mapped == UINT_MAX) mapped = slot_primary_conn_id(hslot);
        // Only route to a different connection if it's actually ready; otherwise
        // fall back to the same connection (CLUSTER SLOTS may still be in flight).
        //
        // The readiness predicate splits by request class to mirror
        // select_target_conn's write-path tolerance and the cross-shard write
        // retry path: writes only need the target's TCP socket connected
        // because the primary owns the slot regardless of the cluster_slots
        // ladder state; reads still need the full is_ready_for_reads() ladder
        // because the slot map must be valid to honor --read-preference.
        // Without this split, a primary mid-CLUSTER-SLOTS-refresh
        // (m_cluster_slots transiently != setup_done) would reject a
        // write-retry and the retry would dribble into the source connection,
        // defeating MOVED's purpose.
        bool target_ready = false;
        if (mapped != UINT_MAX && mapped < m_connections.size()) {
            if (is_read) {
                target_ready = m_connections[mapped]->is_ready_for_reads();
            } else {
                target_ready = m_connections[mapped]->get_connection_state() == conn_connected;
            }
        }
        if (target_ready) {
            target = mapped;
        }
    }

    if (m_connections[target]->enqueue_retry(req)) {
        m_stats.inc_retry_attempt();
        if (req->m_retries == 0) m_stats.inc_retried_op();
        return true;
    }
    return false;
}

// Terminal accounting for a MOVED/ASK request whose retry was refused (e.g.
// max_retries exhausted or retry queue full). Without this, the request would
// disappear from all accounting silently.
void cluster_client::finalize_dropped_redirect(struct timeval timestamp, request *req, protocol_response *response)
{
    if (m_config->failed_keys_file) {
        const char *cmd = "REDIRECT";
        switch (req->m_type) {
        case rt_get:
            cmd = "GET";
            break;
        case rt_set:
            cmd = "SET";
            break;
        case rt_arbitrary:
            cmd = "ARBITRARY";
            break;
        default:
            break;
        }
        global_failed_keys_logger().log_failure(timestamp, cmd, req->m_key, req->m_key_len, response->get_status(),
                                                req->m_retries);
    }
    m_stats.inc_error();
}

void cluster_client::handle_response(unsigned int conn_id, struct timeval timestamp, request *request,
                                     protocol_response *response)
{
    // EWMA latency update for `--read-preference=nearest`. Computed from the
    // request's most-recent send time (m_sent_time), not its first attempt;
    // retries should be reflected in the latency observed by selection.
    // Skip error responses so a flaky replica isn't punished twice (once by
    // unavailability, once by inflated EWMA).
    //
    // Cursor bugbot MED (cluster_client.cpp:2011): rt_arbitrary covers both
    // reads AND writes from the --command path (request type is stamped at
    // creation, before read/write classification). Updating the per-endpoint
    // EWMA on arbitrary writes biased rp_nearest selection toward endpoints
    // that handled writes (always the primary), defeating the
    // nearest-replica intent in mixed arbitrary workloads. Funnel arbitrary
    // requests through classify_read() so only actual reads seed the EWMA;
    // rt_get is always a read by definition. Setup-ladder responses (AUTH,
    // HELLO, CLUSTER SLOTS, READONLY) and built-in writes (rt_set, rt_wait)
    // are still excluded by the outer m_type gate.
    if (!response->is_error() && request != NULL) {
        bool ewma_eligible = false;
        if (request->m_type == rt_get) {
            ewma_eligible = true;
        } else if (request->m_type == rt_arbitrary) {
            ewma_eligible = classify_read(request);
        }
        if (ewma_eligible) {
            const long long diff_us = ts_diff(request->m_sent_time, timestamp);
            if (diff_us > 0) m_connections[conn_id]->update_latency_ewma((double) diff_us);
        }
    }

    if (response->is_error()) {
        benchmark_debug_log("server %s handle response: %s\n", m_connections[conn_id]->get_readable_id(),
                            response->get_status());
        // handle "-READONLY"
        // Server signals "you sent a write to a replica". We misclassified
        // this request as a read (or the topology changed mid-flight, e.g.
        // a failover demoted the primary to a replica). Re-route to the
        // current slot primary and retry once. We do NOT trigger a CLUSTER
        // SLOTS refresh here -- READONLY is a per-request signal, not a
        // topology event, and the cost of bouncing the entire cluster's
        // topology view per misclassification is way too high.
        //
        // Deferred follow-up (#99): the prefix check matches the simple-error
        // form "-READONLY ...". Redis currently emits simple errors for
        // READONLY, but the RESP3 blob-error type "!<len>\r\n-READONLY ..."
        // would not match this byte-prefix and would fall through to the
        // generic error-handling path below (terminal error, no retry). This
        // is acceptable today because Redis OSS does not produce blob errors
        // for READONLY, but revisit if the upstream behavior changes.
        static const char READONLY_MSG_PREFIX[] = "-READONLY";
        static const size_t READONLY_MSG_PREFIX_LEN = sizeof(READONLY_MSG_PREFIX) - 1;
        if (strncmp(response->get_status(), READONLY_MSG_PREFIX, READONLY_MSG_PREFIX_LEN) == 0) {
            benchmark_debug_log("server %s: READONLY (misclassified read); rerouting to primary\n",
                                m_connections[conn_id]->get_readable_id());
            // Override classify_read by stamping request as write before the
            // retry path: -READONLY means the server thinks this is a write,
            // and the next attempt MUST go to a primary regardless of the
            // global --read-preference setting. We do this by force-setting
            // the request type. For arbitrary requests, set is_read_override
            // on the per-request meta via a local flag instead of mutating
            // the shared command_meta (which would affect future calls).
            // Simpler implementation: temporarily set request->m_type to
            // rt_set so classify_read returns false. Restore on exit.
            //
            // Even simpler: just slot-route to the primary directly here,
            // bypassing select_target_conn entirely on the READONLY path.
            //
            // If we cannot identify a *live* primary for this slot (no key /
            // unmapped slot / primary not setup_done), we must NOT fall back
            // to `conn_id` (the rejecting replica) -- doing so re-queues the
            // request onto the same connection that just returned -READONLY,
            // guaranteeing another rejection until --retry-max-attempts is
            // hit. Instead, schedule a topology refresh on this connection
            // (the next CLUSTER SLOTS reply re-binds slot -> primary) and
            // finalize the in-flight request as a terminal error -- the next
            // pipeline tick reroutes future traffic correctly.
            // Match the readiness predicate used by MOVED/ASK write retries
            // and select_target_conn's write path: a TCP-connected primary
            // is sufficient for a write-class retry, even during a
            // CLUSTER SLOTS refresh (cluster_slots_state transiently !=
            // setup_done). Requiring setup_done here would drop valid
            // retries to a primary mid-refresh as terminal errors.
            unsigned int primary_target = UINT_MAX;
            if (request && request->m_key && request->m_key_len > 0) {
                unsigned int hslot = calc_hslot_crc16_with_hash_tag(request->m_key, request->m_key_len);
                unsigned int p = slot_primary_conn_id(hslot);
                if (p != UINT_MAX && p < m_connections.size() &&
                    m_connections[p]->get_connection_state() == conn_connected) {
                    primary_target = p;
                }
            }
            if (primary_target == UINT_MAX) {
                // No live primary known for this slot. Trigger a topology
                // refresh on the rejecting connection and drop the request
                // instead of looping it back onto the same replica.
                m_connections[conn_id]->set_cluster_slots();
                finalize_dropped_redirect(timestamp, request, response);
                return;
            }
            if (m_config->retry_on_error && request && request->m_serialized && request->m_serialized_len > 0 &&
                m_connections[primary_target]->enqueue_retry(request)) {
                m_stats.inc_retry_attempt();
                if (request->m_retries == 0) m_stats.inc_retried_op();
                return;
            }
            // Either retry_on_error is off or no captured bytes. Fall
            // through and finalize as a terminal error to keep accounting
            // honest (the original send did happen on the wire and the
            // server explicitly rejected it).
            finalize_dropped_redirect(timestamp, request, response);
            return;
        }
        // handle "-MOVED"
        if (strncmp(response->get_status(), MOVED_MSG_PREFIX, MOVED_MSG_PREFIX_LEN) == 0) {
            handle_moved(conn_id, timestamp, request, response);
            // With --transaction, retrying a mid-rotation command on the new
            // slot owner would split the MULTI/EXEC block across two shard
            // connections. Drop the command instead. Reset the pin only when the
            // MOVED is on the *current* pin connection: at --pipeline > 1 a later
            // rotation may already hold the pin on another connection, and
            // resetting it would disturb that in-flight rotation. Either way the
            // dropped command is never retried elsewhere (no block split).
            if (m_config->transaction) {
                if ((int) conn_id == m_txn_pinned_conn_id) {
                    if (!m_txn_pin_lost_warned) {
                        m_txn_pin_lost_warned = true;
                        benchmark_error_log("warning: --transaction pin connection (id=%u) received MOVED "
                                            "mid-rotation; topology changed; transaction stats for the "
                                            "interrupted rotation will be inaccurate.\n",
                                            conn_id);
                    }
                    txn_release_pin();
                    for (size_t i = 0; i < m_connections.size(); i++) {
                        if (i != conn_id && m_connections[i]->get_connection_state() != conn_disconnected)
                            m_connections[i]->schedule_fill();
                    }
                } else {
                    benchmark_debug_log("--transaction: MOVED on stale (non-pin) connection %u dropped; "
                                        "current pin=%d left intact\n",
                                        conn_id, m_txn_pinned_conn_id);
                }
                finalize_dropped_redirect(timestamp, request, response);
            } else if (m_config->retry_on_error && !retry_after_redirect(conn_id, request)) {
                // With --retry-on-error, the captured command bytes are resent
                // on the slot-owning connection. MOVED/ASK count toward
                // max_retries. If the retry is refused (budget exhausted /
                // queue full / no captured bytes), account it as a terminal
                // error so the request doesn't silently disappear from stats.
                finalize_dropped_redirect(timestamp, request, response);
            }
            return;
        }

        // handle "-ASK"
        if (strncmp(response->get_status(), ASK_MSG_PREFIX, ASK_MSG_PREFIX_LEN) == 0) {
            handle_ask(conn_id, timestamp, request, response);
            if (m_config->transaction) {
                if ((int) conn_id == m_txn_pinned_conn_id) {
                    if (!m_txn_pin_lost_warned) {
                        m_txn_pin_lost_warned = true;
                        benchmark_error_log("warning: --transaction pin connection (id=%u) received ASK "
                                            "mid-rotation; topology changed; transaction stats for the "
                                            "interrupted rotation will be inaccurate.\n",
                                            conn_id);
                    }
                    txn_release_pin();
                    for (size_t i = 0; i < m_connections.size(); i++) {
                        if (i != conn_id && m_connections[i]->get_connection_state() != conn_disconnected)
                            m_connections[i]->schedule_fill();
                    }
                } else {
                    benchmark_debug_log("--transaction: ASK on stale (non-pin) connection %u dropped; "
                                        "current pin=%d left intact\n",
                                        conn_id, m_txn_pinned_conn_id);
                }
                finalize_dropped_redirect(timestamp, request, response);
            } else if (m_config->retry_on_error && !retry_after_redirect(conn_id, request)) {
                finalize_dropped_redirect(timestamp, request, response);
            }
            return;
        }
    }

    // continue with base class
    client::handle_response(conn_id, timestamp, request, response);
}
