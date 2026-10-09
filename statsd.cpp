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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include <sys/time.h>
#include <event2/util.h>

#include "statsd.h"

statsd_client::statsd_client() : m_socket(-1), m_graphite_port(8080), m_enabled(false), m_initialized(false)
{
    memset(&m_server_addr, 0, sizeof(m_server_addr));
    memset(m_prefix, 0, sizeof(m_prefix));
    memset(m_run_label, 0, sizeof(m_run_label));
    memset(m_graphite_host, 0, sizeof(m_graphite_host));
}

statsd_client::~statsd_client()
{
    close();
}

bool statsd_client::init(const char *host, unsigned short port, const char *prefix, const char *run_label)
{
    if (host == NULL || host[0] == '\0') {
        m_enabled = false;
        return false;
    }

    // Create UDP socket
    m_socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (m_socket < 0) {
        fprintf(stderr, "statsd: failed to create UDP socket: %s\n",
                evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
        return false;
    }

    // Set socket to non-blocking to avoid blocking on send
    evutil_make_socket_nonblocking(m_socket);

    // Resolve hostname
    struct hostent *server = gethostbyname(host);
    if (server == NULL) {
        fprintf(stderr, "statsd: failed to resolve host '%s'\n", host);
        evutil_closesocket(m_socket);
        m_socket = -1;
        return false;
    }

    // Setup server address
    memset(&m_server_addr, 0, sizeof(m_server_addr));
    m_server_addr.sin_family = AF_INET;
    m_server_addr.sin_port = htons(port);
    memcpy(&m_server_addr.sin_addr.s_addr, server->h_addr, server->h_length);

    // Store run label (sanitize: only allow alphanumeric, underscore, hyphen)
    if (run_label != NULL && run_label[0] != '\0') {
        strncpy(m_run_label, run_label, sizeof(m_run_label) - 1);
        m_run_label[sizeof(m_run_label) - 1] = '\0';
        // Sanitize: replace invalid characters with underscore
        for (char *p = m_run_label; *p; p++) {
            if (!isalnum(*p) && *p != '_' && *p != '-') {
                *p = '_';
            }
        }
    } else {
        strncpy(m_run_label, "default", sizeof(m_run_label) - 1);
    }

    // Build prefix with run label: prefix.run_label.
    // Sanitize prefix: allow alphanumeric, underscore, hyphen, and dot (for hierarchy).
    char sanitized_prefix[256];
    if (prefix != NULL && prefix[0] != '\0') {
        strncpy(sanitized_prefix, prefix, sizeof(sanitized_prefix) - 1);
        sanitized_prefix[sizeof(sanitized_prefix) - 1] = '\0';
        for (char *p = sanitized_prefix; *p; p++) {
            if (!isalnum(*p) && *p != '_' && *p != '-' && *p != '.') {
                *p = '_';
            }
        }
        snprintf(m_prefix, sizeof(m_prefix), "%s.%s.", sanitized_prefix, m_run_label);
    } else {
        snprintf(m_prefix, sizeof(m_prefix), "%s.", m_run_label);
    }

    // Store graphite host for events (same host, port set via set_graphite_port())
    strncpy(m_graphite_host, host, sizeof(m_graphite_host) - 1);

    m_enabled = true;
    m_initialized = true;

    fprintf(stderr, "statsd: initialized, sending metrics to %s:%u with prefix '%s' run_label '%s'\n", host, port,
            prefix ? prefix : "", m_run_label);

    return true;
}

void statsd_client::close()
{
    if (m_socket >= 0) {
        evutil_closesocket(m_socket);
        m_socket = -1;
    }
    m_enabled = false;
    m_initialized = false;
}

void statsd_client::send_metric(const char *name, const char *value, const char *type)
{
    if (!is_enabled() || m_socket < 0) {
        return;
    }

    char buffer[512];
    int len = snprintf(buffer, sizeof(buffer), "%s%s:%s|%s", m_prefix, name, value, type);

    if (len > 0 && len < (int) sizeof(buffer)) {
        // Send UDP packet (fire and forget - don't check return value)
        sendto(m_socket, buffer, len, 0, (struct sockaddr *) &m_server_addr, sizeof(m_server_addr));
    }
}

void statsd_client::gauge(const char *name, double value)
{
    char val_str[64];
    snprintf(val_str, sizeof(val_str), "%.6f", value);
    send_metric(name, val_str, "g");
}

void statsd_client::gauge(const char *name, long long value)
{
    char val_str[64];
    snprintf(val_str, sizeof(val_str), "%lld", value);
    send_metric(name, val_str, "g");
}

void statsd_client::timing(const char *name, double value_ms)
{
    char val_str[64];
    snprintf(val_str, sizeof(val_str), "%.3f", value_ms);
    send_metric(name, val_str, "ms");
}

void statsd_client::event(const char *what, const char *data, const char *tags)
{
    if (!is_enabled() || m_graphite_host[0] == '\0') {
        return;
    }

    // Create a TCP socket for HTTP POST to Graphite events API
    evutil_socket_t sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        return;
    }

    // Set socket timeout to avoid blocking
#ifdef _WIN32
    DWORD timeout = 2000;
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (const char *) &timeout, sizeof(timeout));
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char *) &timeout, sizeof(timeout));
#else
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (const char *) &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char *) &tv, sizeof(tv));
#endif

    // Resolve hostname
    struct hostent *server = gethostbyname(m_graphite_host);
    if (server == NULL) {
        evutil_closesocket(sock);
        return;
    }

    // Setup server address
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(m_graphite_port);
    memcpy(&addr.sin_addr.s_addr, server->h_addr, server->h_length);

    // Connect
    if (connect(sock, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
        evutil_closesocket(sock);
        return;
    }

    // Build JSON payload
    char json[1024];
    int json_len;
    if (data != NULL && tags != NULL) {
        json_len = snprintf(json, sizeof(json), "{\"what\":\"%s\",\"tags\":\"%s,run:%s\",\"data\":\"%s\"}", what, tags,
                            m_run_label, data);
    } else if (tags != NULL) {
        json_len = snprintf(json, sizeof(json), "{\"what\":\"%s\",\"tags\":\"%s,run:%s\"}", what, tags, m_run_label);
    } else if (data != NULL) {
        json_len = snprintf(json, sizeof(json), "{\"what\":\"%s\",\"tags\":\"run:%s\",\"data\":\"%s\"}", what,
                            m_run_label, data);
    } else {
        json_len = snprintf(json, sizeof(json), "{\"what\":\"%s\",\"tags\":\"run:%s\"}", what, m_run_label);
    }

    // Build HTTP request
    char request[2048];
    int req_len = snprintf(request, sizeof(request),
                           "POST /events/ HTTP/1.1\r\n"
                           "Host: %s:%u\r\n"
                           "Content-Type: application/json\r\n"
                           "Content-Length: %d\r\n"
                           "Connection: close\r\n"
                           "\r\n"
                           "%s",
                           m_graphite_host, m_graphite_port, json_len, json);

    // Send request (fire and forget) - only if no truncation occurred
    if (req_len > 0 && req_len < (int) sizeof(request)) {
        send(sock, request, req_len, 0);
    }

    evutil_closesocket(sock);
}
