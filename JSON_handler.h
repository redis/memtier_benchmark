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

#ifndef _JSON_HANDLER_H
#define _JSON_HANDLER_H

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <list>

// enum for holding type of nesting
typedef enum
{
    NESTED_GENERAL, // {}
    NESTED_ARRAY    // []
} eJSON_NESTED_TYPE;

/** represents an JSON handler, At this phase, only writes to JSON. */
class json_handler
{
protected:
    FILE *m_json_file;
    // This list is used later for closing the nesting
    std::list<eJSON_NESTED_TYPE> m_nest_closer_types;
    void beutify(bool only_tabs = false);

public:
    json_handler(const char *jsonfilename);
    ~json_handler();

    // Closes all the nesting and the file. Returns 0 if the whole document was
    // written, otherwise an errno value. Further calls return 0.
    int close();

    // Write a single object to JSON
    void write_obj(const char *objectname, const char *format, ...);

    // Starts nesting, the type is used for deciding which charecter to be used for opening and closing
    // the nesting ('{}','[]')
    void open_nesting(const char *objectname, eJSON_NESTED_TYPE type = NESTED_GENERAL);

    // returns the nested level left after closing
    int close_nesting();
};

#endif /* _JSON_HANDLER_H */
