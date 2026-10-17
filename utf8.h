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

#ifndef _UTF8_H
#define _UTF8_H

#include <string>

// Copy of str with each invalid UTF-8 sequence (truncated, overlong,
// surrogate, above U+10FFFF) replaced by U+FFFD. JSON text and the Prometheus
// text exposition format must both be UTF-8, so every user-supplied string
// written to the JSON file or to /metrics goes through it. Valid UTF-8 is
// returned unchanged. Callers that dedup JSON keys apply it first, so keys
// that only differ in invalid bytes stay distinct.
std::string utf8_sanitize(const char *str);

#endif /* _UTF8_H */
