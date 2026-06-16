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
#include <string.h>
#include <cmath> // For std::isfinite
#include "JSON_handler.h"


/**
 * C'tor
 * -----
 * Opens the file named jsonfilename and sets the first nesting as NESTED_GENERAL
 * In case of failure to create the file, will state the error perror
 */
json_handler::json_handler(const char *jsonfilename) : m_json_file(NULL)
{
    // Try to open a file and add the first level
    m_json_file = fopen(jsonfilename, "w");
    if (!m_json_file) {
        perror(jsonfilename);
    }
    // opening the JSON
    fprintf(stderr, "Json file %s created...\n", jsonfilename);
    fprintf(m_json_file, "{");
    m_nest_closer_types.push_back(NESTED_GENERAL);
    beutify();
}

/**
 * D'tor
 * -----
 * Closes all the nesting and the file pointer.
 */
json_handler::~json_handler()
{
    if (m_json_file) {
        // close nesting...
        while (close_nesting())
            ;
        fclose(m_json_file);
        fprintf(stderr, "Json file closed.\n");
    }
}

/**
 * Write singel object named objectname to the JSON with values stated in ...
 * based on the format defined
 * basically uses fprintf with the same parameters.
 */
void json_handler::write_obj(const char *objectname, const char *format, ...)
{
    fprintf(m_json_file, "\"%s\": ", objectname);
    va_list argptr;
    va_start(argptr, format);
    // Check if format includes "%s", indicating a string is expected
    if (strstr(format, "%s") != nullptr) {
        // Use a temporary va_list to check the string argument without advancing the original
        va_list tmp_argptr;
        va_copy(tmp_argptr, argptr);
        const char *str_arg = va_arg(tmp_argptr, const char *);
        va_end(tmp_argptr);

        if (str_arg == nullptr) {
            // Handle NULL strings by writing "null" to the JSON file
            fprintf(m_json_file, "null");
        } else {
            // Print the valid string argument
            vfprintf(m_json_file, format, argptr);
        }
    }
    // Check if the format expects a floating-point number
    else if (strstr(format, "f") != nullptr || strstr(format, "e") != nullptr || strstr(format, "g") != nullptr) {
        va_list tmp_argptr;
        va_copy(tmp_argptr, argptr);
        double value = va_arg(tmp_argptr, double);
        va_end(tmp_argptr);

        // Emit JSON null for any non-finite value (NaN and +/-Inf). Bare
        // 'nan'/'inf' tokens from vfprintf would otherwise produce invalid JSON.
        if (!std::isfinite(value)) {
            fprintf(m_json_file, "null");
        } else {
            vfprintf(m_json_file, format, argptr);
        }
    } else {
        // For other format specifiers, proceed as usual
        vfprintf(m_json_file, format, argptr);
    }
    va_end(argptr);
    beutify();
    fprintf(m_json_file, ",");
}

/**
 * Starts a nesting with a title as defined in objectname
 * in case objectname == NULL it will not add the title and just start nesting
 * The type defines the kind of charecters that will be used
 */
void json_handler::open_nesting(const char *objectname, eJSON_NESTED_TYPE type /*= NESTED_GENERAL*/)
{
    const char *nestStart = (type == NESTED_GENERAL) ? "{" : "[";
    if (objectname != NULL) {
        fprintf(m_json_file, "\"%s\":", objectname);
    }
    fprintf(m_json_file, "%s", nestStart);
    // Adding the type to the nesting closer list to be able to close it properly
    m_nest_closer_types.push_back(type);
    beutify(false);
}

/**
 * Ends the nesting
 * Closes the nesting based on the nesting list
 * Returns = the nested levels left after the closing
 */
int json_handler::close_nesting()
{
    int nest_level = m_nest_closer_types.size();
    if (nest_level > 0) {
        eJSON_NESTED_TYPE type = m_nest_closer_types.back();
        m_nest_closer_types.pop_back();
        // as we assume that the last value is always a ',' or '\n' we need to remove it first
        fseek(m_json_file, -1, SEEK_CUR);
        const char *nestEnd = (type == NESTED_GENERAL) ? "}" : "]";
        fprintf(m_json_file, "%s", nestEnd);
        beutify();
        if (nest_level > 1) {
            fprintf(m_json_file, ",");
        }
    }
    return m_nest_closer_types.size();
}

/**
 * Add tabls and new line (if only_tabs==true will only add tabs)
 */
void json_handler::beutify(bool only_tabs)
{
    if (only_tabs == false) {
        fprintf(m_json_file, "\n");
    }
    int nest_level = m_nest_closer_types.size();
    for (; nest_level > 0; nest_level--) {
        fprintf(m_json_file, "\t");
    }
}
