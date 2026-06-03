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

#ifndef _CONFIG_TYPES_H
#define _CONFIG_TYPES_H

#ifdef HAVE_PTHREAD_H
#include <pthread.h>
#endif

#include <netinet/in.h>

#include <vector>
#include <string>
#include <atomic>
#include <map>

#include "command_meta.h"

struct config_range
{
    int min;
    int max;

    config_range() : min(0), max(0) {}
    config_range(const char *range_str);
    bool is_defined(void) { return max > 0; }
};

struct config_ratio
{
    unsigned int a;
    unsigned int b;

    config_ratio() : a(0), b(0) {}
    config_ratio(const char *ratio_str);
    bool is_defined(void) { return (a > 0 || b > 0); }
};

struct config_quantiles
{
    std::vector<double> quantile_list;
    config_quantiles();
    config_quantiles(const char *ratio_str);
    bool is_defined(void);
    inline std::vector<double>::iterator begin() { return quantile_list.begin(); }
    inline std::vector<double>::iterator end() { return quantile_list.end(); }
};

struct config_weight_list
{
    struct weight_item
    {
        unsigned int size;
        unsigned int weight;
    };

    std::vector<weight_item> item_list;
    std::vector<weight_item>::iterator next_size_iter;
    unsigned int next_size_weight;

    config_weight_list();
    config_weight_list(const char *str);
    config_weight_list(const config_weight_list &copy);
    config_weight_list &operator=(const config_weight_list &rhs);

    bool is_defined(void);
    unsigned int largest(void);
    const char *print(char *buf, int buf_len);
    unsigned int get_next_size(void);
};

struct connect_info
{
    int ci_family;
    int ci_socktype;
    int ci_protocol;
    socklen_t ci_addrlen;
    struct sockaddr *ci_addr;
    char addr_buf[sizeof(struct sockaddr_storage)];
};

struct server_addr
{
    server_addr(const char *hostname, int port, int resolution);
    virtual ~server_addr();

    int get_connect_info(struct connect_info *ci);
    const char *get_last_error(void) const;

protected:
    int resolve(void);
    pthread_mutex_t m_mutex;

    std::string m_hostname;
    int m_port;
    struct addrinfo *m_server_addr;
    struct addrinfo *m_used_addr;
    int m_resolution;
    std::atomic<int> m_last_error; // Atomic to prevent data race between resolve() and get_connect_info()
};

// Forward declaration for object_generator
class object_generator;

#define KEY_PLACEHOLDER "__key__"
#define DATA_PLACEHOLDER "__data__"
#define MONITOR_PLACEHOLDER_PREFIX "__monitor_line"
#define MONITOR_RANDOM_PLACEHOLDER "__monitor_line@__"
#define SCAN_CURSOR_PLACEHOLDER "__scan_cursor__"

enum command_arg_type
{
    const_type = 0,
    key_type = 1,
    data_type = 2,
    monitor_type = 3,
    monitor_random_type = 4,
    undefined_type = 5,
    scan_cursor_type = 6
};

struct command_arg
{
    command_arg(const char *arg, unsigned int arg_len) :
            type(undefined_type), data(arg, arg_len), monitor_index(0), has_key_affixes(false)
    {
        ;
    }
    command_arg_type type;
    std::string data;
    // For monitor_type, stores the index (1-based)
    size_t monitor_index;
    // the prefix and suffix strings are used for mixed key placeholder storing of substrings
    std::string data_prefix;
    std::string data_suffix;
    // optimization flag to avoid runtime checks
    bool has_key_affixes;
};

struct arbitrary_command
{
    arbitrary_command(const char *cmd);

    bool set_key_pattern(const char *pattern_str);
    bool set_ratio(const char *pattern_str);
    bool split_command_to_args();

    // Looks up the command in the static command_meta registry (built from the
    // vendored redis commands.json), evaluates each key_spec against the parsed
    // argv to populate spec_key_positions, and stamps the resolved reply_shape.
    // Emits a warning when the user's __key__ placeholder count disagrees with
    // the spec. Safe to call even when no metadata is available (memcached,
    // module commands) - in that case spec stays nullptr.
    void resolve_command_meta();

    // Number of user-supplied __key__ placeholders, by direct scan over args.
    // Stable regardless of whether format_arbitrary_command has been called yet.
    unsigned int count_user_key_placeholders() const;

    std::vector<command_arg> command_args;
    std::string command;
    std::string command_name; // Display name (e.g., "SET (Line 1)" or "SET")
    std::string command_type; // Base command type for aggregation (e.g., "SET")
    char key_pattern;
    unsigned int keys_count;
    unsigned int ratio;
    bool stats_only; // If true, this is a stats-only slot (not executed, just for stats tracking)

    // Resolved at startup by resolve_command_meta(); see command_meta.h.
    const memtier::command_meta::CommandSpec *spec;
    // 1-based argv positions where keys are expected per the spec. Empty if
    // spec is null or the spec couldn't be evaluated against argv.
    std::vector<size_t> spec_key_positions;
    // True when miss tracking should be performed for this command's replies.
    // Set by resolve_command_meta() (true iff spec exists and reply_shape is
    // miss-bearing). May be cleared later by the --command-miss-tracking flag.
    bool miss_tracking_enabled;

    // Per-command read classification override set by --command-is-read.
    // -1 = not set (inherit global read_preference classification),
    //  0 = force treat as a write,
    //  1 = force treat as a read.
    // C++11-compatible replacement for std::optional<bool>.
    int is_read_override; // -1 / 0 / 1
};

struct arbitrary_command_list
{
private:
    std::vector<arbitrary_command> commands_list;

public:
    arbitrary_command_list() { ; }

    arbitrary_command &at(size_t idx) { return commands_list.at(idx); }
    const arbitrary_command &at(std::size_t idx) const { return commands_list.at(idx); }

    // array subscript operator
    arbitrary_command &operator[](std::size_t idx) { return commands_list[idx]; }
    const arbitrary_command &operator[](std::size_t idx) const { return commands_list[idx]; }

    void add_command(const arbitrary_command &command) { commands_list.push_back(command); }

    arbitrary_command &get_last_command() { return commands_list.back(); }

    size_t size() const { return commands_list.size(); }

    bool is_defined() const { return !commands_list.empty(); }

    unsigned int get_max_command_name_length() const
    {
        unsigned int max_length = 0;

        for (size_t i = 0; i < size(); i++) {
            if (commands_list[i].command_name.length() > max_length) {
                max_length = commands_list[i].command_name.length();
            }
        }

        return max_length;
    }
};

struct monitor_command_list
{
private:
    std::vector<std::string> commands;
    std::vector<std::string> command_types;            // Command type for each command (e.g., "SET", "GET")
    std::map<std::string, size_t> type_to_stats_index; // Maps command type to stats slot index
    std::atomic<size_t> next_index;

public:
    monitor_command_list() : next_index(0) { ; }

    bool load_from_file(const char *filename);
    const std::string &get_command(size_t index) const;
    const std::string &get_random_command(object_generator *obj_gen, size_t *out_index) const;
    const std::string &get_next_sequential_command(size_t *out_index);

    size_t size() const { return commands.size(); }

    // Get unique command types found in the file (for stats allocation)
    std::vector<std::string> get_unique_command_types() const;

    // Set up stats index mapping - called after allocating stats slots
    void setup_stats_indices(size_t base_index);

    // Get the stats index for a command at the given file index
    size_t get_stats_index(size_t cmd_index) const;

    // Get the command type for a command at the given file index
    const std::string &get_command_type(size_t cmd_index) const;
};

#endif /* _CONFIG_TYPES_H */
