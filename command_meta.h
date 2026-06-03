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
#pragma once

#include <cstddef>
#include <cstdint>

namespace memtier
{
namespace command_meta
{

enum class BeginSearchType : uint8_t
{
    Unknown,
    Index,
    Keyword
};
enum class FindKeysType : uint8_t
{
    Unknown,
    Range,
    Keynum
};

enum class ReplyShape : uint8_t
{
    NotMissable,          // SET, DEL, INCR ... no miss concept.
    SingleNullBulk,       // GET, HGET, ZSCORE ... null reply iff key/field absent.
    ArrayPerElementNulls, // MGET, HMGET, ZMSCORE ... per-position nulls.
    EmptyCollection,      // SMEMBERS, LRANGE, HGETALL ... empty array iff missing (heuristic).
    IntegerMembership,    // EXISTS, SISMEMBER, HEXISTS ... integer = number of hits.
    Unknown,              // No reply_schema and not in override table.
};

struct BeginSearch
{
    BeginSearchType type;
    int32_t pos;         // Index.pos (1-based; 0 if unused).
    const char *keyword; // Keyword.keyword (nullptr unless type == Keyword).
    int32_t startfrom;   // Keyword.startfrom.
};

struct FindKeys
{
    FindKeysType type;
    int32_t lastkey;     // Range.lastkey (relative to begin; -1 == to end of argv).
    int32_t step;        // Range.step.
    int32_t limit;       // Range.limit (0 == no limit).
    int32_t keynumidx;   // Keynum.keynumidx.
    int32_t firstkey;    // Keynum.firstkey.
    int32_t keynum_step; // Keynum.step (separate field to keep the layout flat).
};

struct KeySpec
{
    BeginSearch begin;
    FindKeys find;
};

struct CommandSpec
{
    const char *name;      // Uppercase. Subcommands are space-separated, e.g. "XGROUP CREATE".
    int32_t arity;         // Negative arity == variadic minimum.
    bool movable_keys;     // True if command_flags contains MOVABLEKEYS.
    bool is_read;          // True if command_flags contains READONLY (safe to route to replicas).
    uint8_t num_key_specs; // Length of key_specs array.
    const KeySpec *key_specs;
    ReplyShape reply_shape;
};

// Case-insensitive lookup over the static command table. Returns nullptr if
// the name is not registered. For subcommands, pass the canonical
// "CONTAINER SUB" form (e.g. "XGROUP CREATE").
const CommandSpec *lookup(const char *name);

// Number of commands compiled into the static table.
size_t count();

inline const char *reply_shape_name(ReplyShape shape)
{
    switch (shape) {
    case ReplyShape::NotMissable:
        return "NotMissable";
    case ReplyShape::SingleNullBulk:
        return "SingleNullBulk";
    case ReplyShape::ArrayPerElementNulls:
        return "ArrayPerElementNulls";
    case ReplyShape::EmptyCollection:
        return "EmptyCollection";
    case ReplyShape::IntegerMembership:
        return "IntegerMembership";
    case ReplyShape::Unknown:
        return "Unknown";
    default:
        return "?";
    }
}

} // namespace command_meta
} // namespace memtier
