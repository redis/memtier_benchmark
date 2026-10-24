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
#include "command_meta.h"

#include <strings.h>

#include "command_meta_data.h"

namespace memtier
{
namespace command_meta
{

const CommandSpec *lookup(const char *name)
{
    if (name == nullptr) {
        return nullptr;
    }
    // Linear scan is fine: lookup runs once per --command at startup, never on
    // the request path. The static table is on the order of ~500 entries.
    for (size_t i = 0; i < kCommandsCount; ++i) {
        if (strcasecmp(kCommands[i].name, name) == 0) {
            return &kCommands[i];
        }
    }
    return nullptr;
}

size_t count()
{
    return kCommandsCount;
}

} // namespace command_meta
} // namespace memtier
