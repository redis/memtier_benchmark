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

#ifndef _STATSD_H
#define _STATSD_H

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

/**
 * Simple StatsD client for sending metrics over UDP.
 * 
 * StatsD protocol format:
 *   <metric_name>:<value>|<type>[|@<sample_rate>]
 * 
 * Types:
 *   g = gauge (instantaneous value)
 *   c = counter (increment/decrement)
 *   ms = timing (milliseconds)
 *   h = histogram
 *   s = set
 */
class statsd_client {
private:
    int m_socket;
    struct sockaddr_in m_server_addr;
    char m_prefix[256];
    char m_run_label[128];
    char m_graphite_host[256];
    unsigned short m_graphite_port;
    bool m_enabled;
    bool m_initialized;

    // Internal method to send a formatted metric
    void send_metric(const char* name, const char* value, const char* type);

public:
    statsd_client();
    ~statsd_client();

    /**
     * Initialize the StatsD client.
     * @param host StatsD server hostname or IP
     * @param port StatsD server port (default 8125)
     * @param prefix Prefix for all metric names (e.g., "memtier")
     * @param run_label Label for this benchmark run (e.g., "asm_scaling")
     * @return true if initialization successful, false otherwise
     */
    bool init(const char* host, unsigned short port, const char* prefix, const char* run_label);

    /**
     * Get the run label for this client.
     */
    const char* get_run_label() const { return m_run_label; }

    /**
     * Close the UDP socket and cleanup.
     */
    void close();

    /**
     * Check if the client is enabled and ready to send metrics.
     */
    bool is_enabled() const { return m_enabled && m_initialized; }

    /**
     * Send a gauge metric (instantaneous value).
     * @param name Metric name (will be prefixed)
     * @param value Current value
     */
    void gauge(const char* name, double value);

    /**
     * Send a gauge metric with long value.
     */
    void gauge(const char* name, long value);

    /**
     * Send a timing metric (in milliseconds).
     * @param name Metric name (will be prefixed)
     * @param value_ms Time in milliseconds
     */
    void timing(const char* name, double value_ms);

    /**
     * Send a counter metric (increment).
     * @param name Metric name (will be prefixed)
     * @param value Increment value (can be negative for decrement)
     */
    void counter(const char* name, long value);

    /**
     * Send a histogram metric.
     * @param name Metric name (will be prefixed)
     * @param value Sample value
     */
    void histogram(const char* name, double value);

    /**
     * Send an event/annotation to Graphite.
     * This sends an HTTP POST to Graphite's events API.
     * @param what Short description of the event
     * @param data Additional event data (optional)
     * @param tags Comma-separated tags (optional)
     */
    void event(const char* what, const char* data = NULL, const char* tags = NULL);
};

#endif // _STATSD_H

