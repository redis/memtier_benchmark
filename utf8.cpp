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

#include <stddef.h>

#include "utf8.h"

/**
 * Return the length of the valid UTF-8 sequence at p, or 0 if p does not
 * start one. In that case *subpart is set to the length of the maximal
 * subpart (the longest prefix of a valid sequence, at least 1 byte), which
 * is replaced by a single U+FFFD as Unicode recommends. Overlongs,
 * surrogates (U+D800..U+DFFF) and code points above U+10FFFF are invalid.
 * The string's NUL terminator ends a truncated sequence.
 */
static size_t utf8_sequence_length(const unsigned char *p, size_t *subpart)
{
    unsigned char lo = 0x80, hi = 0xBF; // valid range of the second byte
    size_t len;
    if (p[0] >= 0xC2 && p[0] <= 0xDF) {
        len = 2;
    } else if (p[0] >= 0xE0 && p[0] <= 0xEF) {
        len = 3;
        if (p[0] == 0xE0) lo = 0xA0; // overlong
        if (p[0] == 0xED) hi = 0x9F; // surrogate
    } else if (p[0] >= 0xF0 && p[0] <= 0xF4) {
        len = 4;
        if (p[0] == 0xF0) lo = 0x90; // overlong
        if (p[0] == 0xF4) hi = 0x8F; // above U+10FFFF
    } else {
        *subpart = 1;
        return 0;
    }
    for (size_t i = 1; i < len; i++) {
        if (p[i] < lo || p[i] > hi) {
            *subpart = i;
            return 0;
        }
        lo = 0x80;
        hi = 0xBF;
    }
    return len;
}

std::string utf8_sanitize(const char *str)
{
    std::string out;
    const unsigned char *p = (const unsigned char *) str;
    while (*p) {
        if (*p < 0x80) {
            out += (char) *p++;
            continue;
        }
        size_t subpart;
        size_t len = utf8_sequence_length(p, &subpart);
        if (len > 0) {
            out.append((const char *) p, len);
            p += len;
        } else {
            out += "\xEF\xBF\xBD"; // U+FFFD REPLACEMENT CHARACTER
            p += subpart;
        }
    }
    return out;
}
