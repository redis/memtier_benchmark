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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <errno.h>

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#include <netdb.h>

#include <string>
#include <iostream>
#include <stdexcept>
#include <climits>
#include <algorithm>

#include "config_types.h"
#include "obj_gen.h"

config_range::config_range(const char *range_str) : min(0), max(0)
{
    assert(range_str != NULL);

    char *p = NULL;
    min = strtoul(range_str, &p, 10);
    if (!p || *p != '-') {
        min = max = 0;
        return;
    }

    char *q = NULL;
    max = strtoul(p + 1, &q, 10);
    if (!q || *q != '\0') {
        min = max = 0;
        return;
    }

    if (min > max) {
        int tmp = min;
        min = max;
        max = tmp;
    }
}

config_ratio::config_ratio(const char *ratio_str) : a(0), b(0)
{
    assert(ratio_str != NULL);

    char *p = NULL;
    a = strtoul(ratio_str, &p, 10);
    if (!p || *p != ':') {
        a = b = 0;
        return;
    }

    char *q = NULL;
    b = strtoul(p + 1, &q, 10);
    if (!q || *q != '\0') {
        a = b = 0;
        return;
    }
}

config_quantiles::config_quantiles() {}

config_quantiles::config_quantiles(const char *str)
{
    assert(str != NULL);

    do {
        double quantile;
        char *p = NULL;
        quantile = strtod(str, &p);
        if (!p || (*p != ',' && *p != '\0')) {
            quantile_list.clear();
            return;
        }
        str = p;
        if (*str) str++;
        quantile_list.push_back(quantile);
    } while (*str);
}

bool config_quantiles::is_defined(void)
{
    return quantile_list.size() > 0;
}


config_weight_list::config_weight_list() : next_size_weight(0) {}

config_weight_list::config_weight_list(const config_weight_list &copy) : next_size_weight(0)
{
    for (std::vector<weight_item>::const_iterator i = copy.item_list.begin(); i != copy.item_list.end(); i++) {
        const weight_item wi = *i;
        item_list.push_back(wi);
    }
    next_size_iter = item_list.begin();
}

config_weight_list &config_weight_list::operator=(const config_weight_list &rhs)
{
    if (this == &rhs) return *this;

    next_size_weight = rhs.next_size_weight;
    for (std::vector<weight_item>::const_iterator i = rhs.item_list.begin(); i != rhs.item_list.end(); i++) {
        const weight_item wi = *i;
        item_list.push_back(wi);
    }
    next_size_iter = item_list.begin();
    return *this;
}

config_weight_list::config_weight_list(const char *str) : next_size_weight(0)
{
    assert(str != NULL);

    do {
        struct weight_item w;
        char *p = NULL;
        w.size = strtoul(str, &p, 10);
        if (!p || *p != ':') {
            item_list.clear();
            return;
        }

        str = p + 1;
        w.weight = strtoul(str, &p, 10);
        if (!p || (*p != ',' && *p != '\0')) {
            item_list.clear();
            return;
        }

        str = p;
        if (*str) str++;
        item_list.push_back(w);
    } while (*str);

    next_size_iter = item_list.begin();
}

bool config_weight_list::is_defined(void)
{
    if (item_list.size() > 0) return true;
    return false;
}

unsigned int config_weight_list::largest(void)
{
    unsigned int largest = 0;
    for (std::vector<weight_item>::iterator i = item_list.begin(); i != item_list.end(); i++) {
        if (i->size > largest) largest = i->size;
    }

    return largest;
}

unsigned int config_weight_list::get_next_size(void)
{
    while (next_size_weight >= next_size_iter->weight) {
        next_size_iter++;
        next_size_weight = 0;
        if (next_size_iter == item_list.end()) {
            next_size_iter = item_list.begin();
        }
    }

    next_size_weight++;
    return next_size_iter->size;
}

const char *config_weight_list::print(char *buf, int buf_len)
{
    const char *start = buf;
    assert(buf != NULL && buf_len > 0);

    *buf = '\0';
    for (std::vector<weight_item>::iterator i = item_list.begin(); i != item_list.end(); i++) {
        int n = snprintf(buf, buf_len, "%s%u:%u", i != item_list.begin() ? "," : "", i->size, i->weight);
        buf += n;
        buf_len -= n;
        if (!buf_len) return NULL;
    }

    return start;
}


server_addr::server_addr(const char *hostname, int port, int resolution) :
        m_hostname(hostname),
        m_port(port),
        m_server_addr(NULL),
        m_used_addr(NULL),
        m_resolution(resolution),
        m_last_error(0)
{
    int error = resolve();

    if (error != 0) throw std::runtime_error(std::string(gai_strerror(error)));

    pthread_mutex_init(&m_mutex, NULL);
}

server_addr::~server_addr()
{
    if (m_server_addr) {
        freeaddrinfo(m_server_addr);
        m_server_addr = NULL;
    }

    pthread_mutex_destroy(&m_mutex);
}

int server_addr::resolve(void)
{
    char port_str[20];
    struct addrinfo hints;

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = m_resolution;

    snprintf(port_str, sizeof(port_str) - 1, "%u", m_port);
    m_last_error = getaddrinfo(m_hostname.c_str(), port_str, &hints, &m_server_addr);
    return m_last_error;
}

int server_addr::get_connect_info(struct connect_info *ci)
{
    pthread_mutex_lock(&m_mutex);
    if (m_used_addr) m_used_addr = m_used_addr->ai_next;
    if (!m_used_addr) {
        if (m_server_addr) {
            freeaddrinfo(m_server_addr);
            m_server_addr = NULL;
        }
        if (resolve() == 0) {
            m_used_addr = m_server_addr;
        } else {
            m_used_addr = NULL;
        }
    }

    if (m_used_addr) {
        ci->ci_family = m_used_addr->ai_family;
        ci->ci_socktype = m_used_addr->ai_socktype;
        ci->ci_protocol = m_used_addr->ai_protocol;
        assert(m_used_addr->ai_addrlen <= sizeof(ci->addr_buf));
        memcpy(ci->addr_buf, m_used_addr->ai_addr, m_used_addr->ai_addrlen);
        ci->ci_addr = (struct sockaddr *) ci->addr_buf;
        ci->ci_addrlen = m_used_addr->ai_addrlen;
    }
    pthread_mutex_unlock(&m_mutex);
    return m_last_error;
}

const char *server_addr::get_last_error(void) const
{
    return gai_strerror(m_last_error);
}

static int hex_digit_to_int(char c)
{
    if (c >= 'a' && c <= 'f') {
        return (c - 'a') + 10;
    } else if (c >= 'A' && c <= 'F') {
        return (c - 'A') + 10;
    } else if (c >= '0' && c <= '9') {
        return (c - '0');
    } else {
        return -1;
    }
}

arbitrary_command::arbitrary_command(const char *cmd) :
        command(cmd),
        key_pattern('R'),
        keys_count(0),
        ratio(1),
        stats_only(false),
        spec(NULL),
        miss_tracking_enabled(false)
{
    // command name is the first word in the command
    size_t pos = command.find(" ");
    if (pos == std::string::npos) {
        pos = command.size();
    }

    command_name.assign(command.c_str(), pos);
    std::transform(command_name.begin(), command_name.end(), command_name.begin(), ::toupper);
    // command_type is the same as command_name by default (used for aggregation)
    command_type = command_name;
}

unsigned int arbitrary_command::count_user_key_placeholders() const
{
    unsigned int n = 0;
    for (size_t i = 0; i < command_args.size(); ++i) {
        if (command_args[i].data.find(KEY_PLACEHOLDER) != std::string::npos) n++;
    }
    return n;
}

// Evaluate one key_spec against argv and append the discovered 1-based key
// positions to `out`. Returns false when the spec can't be evaluated (unknown
// shape, missing keyword, unparseable keynum count, etc.).
static bool evaluate_key_spec(const memtier::command_meta::KeySpec &ks, const std::vector<command_arg> &args,
                              std::vector<size_t> &out)
{
    using namespace memtier::command_meta;

    int argc = (int) args.size();
    int start_pos = 0; // 1-based (Redis convention: pos 0 = command name)

    // Indexing convention: commands.json positions are 1-based with position 0
    // being the command name; in our 0-based command_args vector that maps
    // directly (Redis position N = command_args[N], no offset). Earlier code
    // used args[idx - 1] which read the wrong slot — broken for Keyword
    // (always read the command name first) and Keynum (read the wrong
    // argument as the keynum count, e.g. EVAL's "script" instead of
    // "numkeys"). Range never accessed args[] so the bug was latent.

    // BeginSearch: where does the key region begin?
    if (ks.begin.type == BeginSearchType::Index) {
        start_pos = ks.begin.pos;
    } else if (ks.begin.type == BeginSearchType::Keyword) {
        // commands.json startfrom is 1-based: positive == forward from that index,
        // negative == backward from end (-1 means start at the last argv slot).
        int from = ks.begin.startfrom;
        int direction = (from < 0) ? -1 : 1;
        int idx = (from < 0) ? (argc + from) : from;
        if (idx < 1) idx = 1;
        if (idx >= argc) idx = argc - 1;
        for (; idx >= 1 && idx < argc; idx += direction) {
            if (ks.begin.keyword != NULL && strcasecmp(args[idx].data.c_str(), ks.begin.keyword) == 0) {
                // Keys begin AFTER the keyword token.
                start_pos = idx + 1;
                break;
            }
        }
        if (start_pos == 0) return false;
    } else {
        return false;
    }

    if (start_pos < 1 || start_pos >= argc) return false;

    // FindKeys: enumerate keys from start_pos.
    if (ks.find.type == FindKeysType::Range) {
        int last;
        if (ks.find.lastkey >= 0) {
            last = start_pos + ks.find.lastkey;
        } else {
            // Negative lastkey: relative to end of argv. -1 == "last argv slot".
            // The last valid key index is argc-1 (since command_args[0] is the
            // command name), so lastkey=-1 maps to argc-1, -2 to argc-2, etc.
            last = argc - 1 + ks.find.lastkey + 1;
        }
        if (last >= argc) last = argc - 1;
        if (last < start_pos) return true; // empty range; nothing to add
        int step = ks.find.step > 0 ? ks.find.step : 1;
        // Total positions in the range, considering stride: positions are
        // start_pos, start_pos+step, start_pos+2*step, ..., up to last.
        int total = (last - start_pos) / step + 1;
        // limit halves (or further divides) the available positions when nonzero.
        if (ks.find.limit > 0) {
            total = total / ks.find.limit;
            // Recompute last so it lands on the strided position, not on
            // last-1 contiguous slots (matters once a Redis command pairs
            // limit > 0 with step > 1; today none do — XREAD/XREADGROUP both
            // use step=1 — so this is a defensive correctness fix).
            last = start_pos + (total - 1) * step;
        }
        for (int p = start_pos; p <= last; p += step) {
            out.push_back((size_t) p);
        }
        return true;
    } else if (ks.find.type == FindKeysType::Keynum) {
        int numidx = start_pos + ks.find.keynumidx;
        if (numidx < 1 || numidx >= argc) return false;
        // Try parsing the arg value as the count. Placeholders (e.g. __data__)
        // are unparseable and we bail out without populating positions.
        const std::string &numstr = args[numidx].data;
        char *end = NULL;
        long count = strtol(numstr.c_str(), &end, 10);
        if (numstr.empty() || end == numstr.c_str() || *end != '\0' || count < 0) {
            return false;
        }
        int firstkey = start_pos + ks.find.firstkey;
        int step = ks.find.keynum_step > 0 ? ks.find.keynum_step : 1;
        for (long i = 0; i < count; ++i) {
            int p = firstkey + (int) (i * step);
            if (p < 1 || p >= argc) break;
            out.push_back((size_t) p);
        }
        return true;
    }
    return false;
}

void arbitrary_command::resolve_command_meta()
{
    using namespace memtier::command_meta;

    spec = NULL;
    spec_key_positions.clear();
    miss_tracking_enabled = false;

    if (command_args.empty()) {
        return;
    }
    // The canonical name for subcommand containers (XGROUP CREATE, OBJECT FREQ,
    // etc.) is the first two argv tokens uppercased and space-joined. Try both
    // forms and keep whichever resolves.
    std::string two_word_name;
    if (command_args.size() >= 2) {
        two_word_name.assign(command_args[0].data);
        two_word_name.push_back(' ');
        two_word_name.append(command_args[1].data);
        std::transform(two_word_name.begin(), two_word_name.end(), two_word_name.begin(), ::toupper);
        spec = lookup(two_word_name.c_str());
    }
    if (spec == NULL) {
        spec = lookup(command_name.c_str());
    }
    if (spec == NULL) {
        // No metadata; this is fine for memcached / module / unknown commands.
        return;
    }

    // Evaluate each key spec against the user's argv.
    for (uint8_t i = 0; i < spec->num_key_specs; ++i) {
        evaluate_key_spec(spec->key_specs[i], command_args, spec_key_positions);
    }

    // Cross-check against user's __key__ placeholders. Mismatches are not fatal
    // (the user might be using a custom routing scheme or a module); just warn.
    unsigned int user_keys = count_user_key_placeholders();
    if (!spec_key_positions.empty() && user_keys != spec_key_positions.size()) {
        fprintf(stderr,
                "warning: --command \"%s\": spec for %s expects %zu key(s) but %u __key__ placeholder(s) "
                "were supplied; miss tracking will follow the spec.\n",
                command.c_str(), spec->name, spec_key_positions.size(), user_keys);
    }

    // Default-enable miss tracking when the command has a miss-bearing reply
    // shape. The --command-miss-tracking flag may override this later.
    switch (spec->reply_shape) {
    case ReplyShape::SingleNullBulk:
    case ReplyShape::ArrayPerElementNulls:
    case ReplyShape::EmptyCollection:
    case ReplyShape::IntegerMembership:
        miss_tracking_enabled = true;
        break;
    default:
        miss_tracking_enabled = false;
        break;
    }
}

bool arbitrary_command::set_key_pattern(const char *pattern_str)
{
    if (strlen(pattern_str) > 1) {
        return false;
    }

    if (pattern_str[0] != 'R' && pattern_str[0] != 'G' && pattern_str[0] != 'Z' && pattern_str[0] != 'S' &&
        pattern_str[0] != 'P') {
        return false;
    }

    key_pattern = pattern_str[0];
    return true;
}

bool arbitrary_command::set_ratio(const char *ratio_str)
{
    // Reject empty / null / whitespace-only / negative input. strtoul()
    // silently accepts the empty string (returns 0) and wraps negatives to
    // a huge unsigned value, both of which lead to a worker loop that never
    // picks this command and hangs forever (issue #426 item 14).
    if (ratio_str == NULL || *ratio_str == '\0') {
        return false;
    }
    const char *first = ratio_str;
    while (*first && isspace((unsigned char) *first)) {
        first++;
    }
    if (*first == '\0' || *first == '-' || *first == '+') {
        return false;
    }

    char *q = NULL;
    errno = 0;
    unsigned long parsed = strtoul(ratio_str, &q, 10);
    if (!q || *q != '\0') {
        return false;
    }
    // ERANGE catches the strtoul overflow case; the narrow-to-unsigned-int
    // check catches values above UINT_MAX that strtoul accepted on a 64-bit
    // unsigned long without ERANGE (e.g. "4294967296" on x86_64). Without
    // either guard a too-large value silently truncates and a wrap-to-0
    // value re-introduces the very hang we're guarding against.
    if (errno == ERANGE || parsed == 0 || parsed > UINT_MAX) {
        return false;
    }

    ratio = (unsigned int) parsed;
    return true;
}

bool arbitrary_command::split_command_to_args()
{
    const char *p = command.c_str();
    size_t command_len = command.length();

    // Heap-allocated scratch buffer; we used to declare `char buffer[command_len]`
    // here, but with --monitor-input replays this is called per request with
    // attacker-controlled length, so a multi-MB MONITOR-captured value (e.g.
    // an HSET storing a gzipped blob) would blow the worker thread's stack.
    std::vector<char> buffer(command_len);
    unsigned int buffer_len = 0;

    while (1) {
        /* skip blanks */
        while (*p && isspace(*p)) {
            p++;
        }

        if (*p) {
            /* get a token */
            bool in_quotes = 0;        /* set to 1 if we are in "quotes" */
            bool in_single_quotes = 0; /* set to 1 if we are in 'single quotes' */
            bool done = 0;
            buffer_len = 0;
            // current = p;

            while (!done) {
                if (in_quotes) {
                    if (*p == '\\' && *(p + 1) == 'x' && isxdigit(*(p + 2)) && isxdigit(*(p + 3))) {
                        unsigned char byte;
                        byte = (hex_digit_to_int(*(p + 2)) * 16) + hex_digit_to_int(*(p + 3));

                        buffer[buffer_len] = byte;
                        buffer_len++;
                        p += 3;
                    } else if (*p == '\\' && *(p + 1)) {
                        char c;
                        p++;

                        switch (*p) {
                        case 'n':
                            c = '\n';
                            break;

                        case 'r':
                            c = '\r';
                            break;

                        case 't':
                            c = '\t';
                            break;

                        case 'b':
                            c = '\b';
                            break;

                        case 'a':
                            c = '\a';
                            break;

                        default:
                            c = *p;
                            break;
                        }

                        buffer[buffer_len] = c;
                        buffer_len++;
                    } else if (*p == '"') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p + 1) && !isspace(*(p + 1))) {
                            goto err;
                        }

                        done = 1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        buffer[buffer_len] = *p;
                        buffer_len++;
                    }
                } else if (in_single_quotes) {
                    if (*p == '\\' && *(p + 1) == '\'') {
                        p++;
                        buffer[buffer_len] = '\'';
                        buffer_len++;
                    } else if (*p == '\'') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p + 1) && !isspace(*(p + 1))) {
                            goto err;
                        }

                        done = 1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        buffer[buffer_len] = *p;
                        buffer_len++;
                    }
                } else {
                    switch (*p) {
                    case ' ':
                    case '\n':
                    case '\r':
                    case '\t':
                    case '\0':
                        done = 1;
                        break;

                    case '"':
                        in_quotes = 1;
                        break;

                    case '\'':
                        in_single_quotes = 1;
                        break;

                    default:
                        buffer[buffer_len] = *p;
                        buffer_len++;
                        break;
                    }
                }

                if (*p) {
                    p++;
                }
            }

            // add new arg
            command_arg arg(buffer.data(), buffer_len);
            command_args.push_back(arg);
        } else {
            return true;
        }
    }

err:
    return false;
}

// Monitor command list implementation

// Helper function to extract command type (first word) from a monitor command string
static std::string extract_command_type(const std::string &command_str)
{
    // Command format: "SET" "key" "value" or "GET" "key"
    // Find the first word between quotes
    size_t start = command_str.find('"');
    if (start == std::string::npos) {
        return "";
    }
    start++; // Skip the opening quote
    size_t end = command_str.find('"', start);
    if (end == std::string::npos) {
        return "";
    }
    std::string cmd_type = command_str.substr(start, end - start);
    // Convert to uppercase
    std::transform(cmd_type.begin(), cmd_type.end(), cmd_type.begin(), ::toupper);
    return cmd_type;
}

bool monitor_command_list::load_from_file(const char *filename)
{
    FILE *file = fopen(filename, "r");
    if (!file) {
        fprintf(stderr, "error: failed to open monitor input file: %s\n", filename);
        return false;
    }

    char *line = NULL;
    size_t line_capacity = 0;
    ssize_t line_len;
    size_t total_lines = 0;

    // Use getline() for dynamic allocation - handles arbitrarily long lines
    while ((line_len = getline(&line, &line_capacity, file)) != -1) {
        // Normalise CR-only line endings (\r without \n) by replacing every
        // bare \r with \n so that the segment loop below works uniformly.
        // getline already consumed the terminating \n (if any), so a trailing
        // \r here is a CRLF remnant stripped below; internal \r bytes are the
        // classic-Mac / some-Windows-export case that must be split on.
        for (ssize_t i = 0; i < line_len - 1; i++) {
            if (line[i] == '\r' && line[i + 1] != '\n') {
                line[i] = '\n';
            }
        }

        // Process each \n-delimited segment within the (possibly rewritten) buffer.
        // In the common case there is exactly one segment and no extra allocation occurs.
        char *seg_start = line;
        char *seg_end;
        while (seg_start < line + line_len) {
            seg_end = (char *) memchr(seg_start, '\n', line + line_len - seg_start);
            size_t seg_len = seg_end ? (size_t) (seg_end - seg_start) : (size_t) (line + line_len - seg_start);

            total_lines++;
            // Find the first quote - this is where the command starts
            char *first_quote = (char *) memchr(seg_start, '"', seg_len);
            if (!first_quote) {
                seg_start = seg_end ? seg_end + 1 : line + line_len;
                continue; // Skip segments without commands
            }

            // Extract everything from first quote to end of segment.
            // Use the explicit byte-count constructor so that embedded \0
            // bytes in binary blobs are preserved (the C-string ctor would
            // silently truncate at the first \0).
            size_t cmd_len = seg_len - (size_t) (first_quote - seg_start);
            std::string command_str(first_quote, cmd_len);

            // Remove trailing newline / carriage-return if present
            if (!command_str.empty() && command_str[command_str.length() - 1] == '\n') {
                command_str.erase(command_str.length() - 1);
            }
            if (!command_str.empty() && command_str[command_str.length() - 1] == '\r') {
                command_str.erase(command_str.length() - 1);
            }

            commands.push_back(command_str);

            // Extract and store the command type (e.g., "SET", "GET")
            std::string cmd_type = extract_command_type(command_str);
            command_types.push_back(cmd_type);

            seg_start = seg_end ? seg_end + 1 : line + line_len;
        }
    }

    free(line);
    fclose(file);

    if (commands.empty()) {
        fprintf(stderr, "error: no commands found in monitor input file: %s\n", filename);
        return false;
    }

    fprintf(stderr, "Loaded %zu monitor commands from %zu total lines\n", commands.size(), total_lines);
    return true;
}

const std::string &monitor_command_list::get_command(size_t index) const
{
    if (index >= commands.size()) {
        static std::string empty;
        return empty;
    }
    return commands[index];
}

const std::string &monitor_command_list::get_random_command(object_generator *obj_gen, size_t *out_index) const
{
    if (commands.empty()) {
        static std::string empty;
        if (out_index) *out_index = 0;
        return empty;
    }
    // Use object_generator's random which respects --randomize and --distinct-client-seed
    size_t random_index = obj_gen->random_range(0, commands.size() - 1);
    if (out_index) *out_index = random_index;
    return commands[random_index];
}

const std::string &monitor_command_list::get_next_sequential_command(size_t *out_index)
{
    if (commands.empty()) {
        static std::string empty;
        if (out_index) *out_index = 0;
        return empty;
    }
    // Use a global sequential index across all clients/threads.
    size_t index = next_index.fetch_add(1, std::memory_order_relaxed);
    index = index % commands.size();
    if (out_index) *out_index = index;
    return commands[index];
}

std::vector<std::string> monitor_command_list::get_unique_command_types() const
{
    std::vector<std::string> unique_types;
    for (const auto &type : command_types) {
        if (!type.empty() && std::find(unique_types.begin(), unique_types.end(), type) == unique_types.end()) {
            unique_types.push_back(type);
        }
    }
    return unique_types;
}

void monitor_command_list::setup_stats_indices(size_t base_index)
{
    // Build mapping from command type to stats index
    type_to_stats_index.clear();
    std::vector<std::string> unique_types = get_unique_command_types();
    for (size_t i = 0; i < unique_types.size(); i++) {
        type_to_stats_index[unique_types[i]] = base_index + i;
    }
}

size_t monitor_command_list::get_stats_index(size_t cmd_index) const
{
    if (cmd_index >= command_types.size()) {
        return 0;
    }
    const std::string &type = command_types[cmd_index];
    auto it = type_to_stats_index.find(type);
    if (it != type_to_stats_index.end()) {
        return it->second;
    }
    return 0; // Fallback (should not happen if setup_stats_indices was called)
}

const std::string &monitor_command_list::get_command_type(size_t cmd_index) const
{
    if (cmd_index >= command_types.size()) {
        static std::string empty;
        return empty;
    }
    return command_types[cmd_index];
}
