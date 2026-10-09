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

#include <string.h>
#include <stdlib.h>
#include "file_io.h"

/** largest support line length */
#define MAX_LINE_BUFFER 1024

/** \brief file_reader constructor.
 * \param filename name of file to open.
 */
file_reader::file_reader(const char *filename) : m_filename(filename), m_file(NULL), m_line(2) {}

/** \brief file_reader destructor.
 */
file_reader::~file_reader()
{
    if (m_file != NULL) {
        fclose(m_file);
    }
}

file_reader::file_reader(const file_reader &from) : m_filename(from.m_filename), m_file(NULL), m_line(2) {}

/** \brief open file and prepare to read items.
 *
 * this method reads the file header and verifies that it is valid.
 * \return true for success, false for error.
 */

bool file_reader::open_file(void)
{
    char header_line[80];
    const char expected_header_line[] = "dumpflags, time, exptime";
    if (!m_filename) return false;

    if (m_file != NULL) fclose(m_file);

    m_file = fopen(m_filename, "rb");
    if (!m_file) {
        perror(m_filename);
        return false;
    }

    if (fgets(header_line, sizeof(header_line) - 1, m_file) == NULL) {
        perror(m_filename);
        return false;
    }

    if (memcmp(header_line, expected_header_line, strlen(expected_header_line)) != 0) {
        fprintf(stderr, "%s: invalid file, unexpected CSV header.\n", m_filename);
        return false;
    }

    return true;
}

/** \brief read a string from file and return it in an allocated buffer.
 *
 * This method does de-quoting of strings, if the string is stored in quoted
 * form.  Quoted strings are defined as those strings that begin with a '"' character.
 *
 * When reading quoted strings, this method supports strings that have NUL characters
 * in them, as well as CR/CRLF characters (i.e. unquoted line breaks).
 *
 * It is expected that the specified string length will be equal to the length
 * of the CSV field (i.e., delimited by a comma and/or terminated by an end quote,
 * for quoted strings).
 *
 * \param len expected string length.
 * \param alloc_len length of buffer to allocate (must be >= len).
 * \param actual_len pointer to unsigned int in which the actual string length read is returned
 * \return pointer to allocated buffer, or NULL on error.
 */

char *file_reader::read_string(unsigned int len, unsigned int alloc_len, unsigned int *actual_len)
{
    char *dest_str = NULL;
    char *d;
    bool skip_quote = false;
    bool first_byte = true;
    bool dequote = false;

    d = dest_str = (char *) malloc(alloc_len);
    while (len > 0) {
        int c = fgetc(m_file);

        if (first_byte) {
            first_byte = false;
            if (c == '"') {
                dequote = true;
                continue;
            }
        }

        // did we hit the eof?
        if (c == EOF) {
            fprintf(stderr, "%s:%d: premature end of file.\n", m_filename, m_line);
            free(dest_str);
            return NULL;
        }

        if (skip_quote && c != '"') {
            break;
        }

        if (c == '"') {
            if (skip_quote) {
                skip_quote = false;
                continue;
            }
            if (dequote) {
                skip_quote = true;
            } else {
                break;
            }
        }

        if (c == ',' && !dequote) {
            fseek(m_file, -1, SEEK_CUR);
            break;
        }
        if ((c == '\r' || c == '\n') && !dequote) {
            fseek(m_file, -1, SEEK_CUR);
            break;
        }

        // copy
        *d = (char) c;
        d++;
        len--;
    }

    if (len > 0) {
        fprintf(stderr, "%s:%d: warning: premature end of string (%d bytes left)\n", m_filename, m_line, len);
    }

    // read ending quote
    if (len == 0 && dequote) {
        if (skip_quote) {
            int c = fgetc(m_file);
            if (c != '"') {
                fprintf(stderr, "%s:%d: warning: string terminated in a quoted character.\n", m_filename, m_line);
            }
        }

        int c = fgetc(m_file);
        if (c != '"') {
            fprintf(stderr, "%s:%d: warning: missing '\"' at end of column (got '%c').\n", m_filename, m_line, c);
        }
    }

    if (actual_len != NULL) *actual_len = (d - dest_str);

    return dest_str;
}

/** \brief determine if end of file has been reached.
 * \return true on EOF, false otherwise.
 */
bool file_reader::is_eof(void)
{
    return (feof(m_file) != 0);
}

/** \brief read the next memcache_item object from the opened file.
 * \return pointer to heap-allocated object, or NULL if error/no more items in file.
 */

memcache_item *file_reader::read_item(void)
{
    // parse next line
    unsigned int s_dumpflags = 0;
    unsigned int s_time = 0;
    unsigned int s_exptime = 0;
    unsigned int s_nbytes = 0;
    unsigned int s_nsuffix = 0;
    unsigned int s_flags = 0;
    unsigned int s_clsid = 0;
    unsigned int s_nkey = 0;

    // scan int values
    if (fscanf(m_file, "%u, %u, %u, %u, %u, %u, %u, %u, ", &s_dumpflags, &s_time, &s_exptime, &s_nbytes, &s_nsuffix,
               &s_flags, &s_clsid, &s_nkey) < 8) {
        if (is_eof()) return NULL;

        fprintf(stderr, "%s:%u: error parsing item values.\n", m_filename, m_line);
        return NULL;
    }

    // read key
    unsigned int key_actlen = 0;
    char *key = read_string(s_nkey, s_nkey + 1, &key_actlen);
    if (key_actlen != s_nkey) {
        fprintf(stderr, "%s:%u: warning: key column is %u bytes, expected %u bytes.\n", m_filename, m_line, key_actlen,
                s_nkey);
    }
    key[s_nkey] = '\0';

    // read data
    int c = fgetc(m_file);
    if (c != ',') {
        fprintf(stderr, "%s:%u: error parsing csv file, got '%c' instead of delmiter.\n", m_filename, m_line, c);
        free(key);
        return NULL;
    }
    fgetc(m_file);

    unsigned int data_actlen = 0;
    char *data = read_string(s_nbytes - 2, s_nbytes, &data_actlen);
    if (data_actlen != s_nbytes - 2) {
        fprintf(stderr, "%s:%u: warning: data column is %u bytes, expected %u bytes.\n", m_filename, m_line,
                data_actlen, s_nbytes);
        free(key);
        free(data);
        return NULL;
    }
    data[s_nbytes - 2] = '\r';
    data[s_nbytes - 1] = '\n';

    // handle end of line
    c = fgetc(m_file);
    if (c == '\r') {
        c = fgetc(m_file);
    }
    if (c != '\n') {
        fprintf(stderr, "%s:%u: warning: end of line expected but not found.\n", m_filename, m_line);
    }

    m_line++;

    // return item
    memcache_item *item = new memcache_item(s_dumpflags, s_time, s_exptime, s_flags, s_nsuffix, s_clsid);
    item->set_key(key, s_nkey);
    item->set_data(data, s_nbytes);

    return item;
}

/////////////////////////////////////////////////////////////////////

/** \brief file_writer constructor.
 * \param filename name of file to open.
 */

file_writer::file_writer(const char *filename) : m_filename(filename), m_file(NULL) {}

/** \brief file_writer destructor.
 *
 * Only when the file_writer object is destructed, the file is closed and any
 * pending buffer is flushed.
 */

file_writer::~file_writer()
{
    if (m_file != NULL) fclose(m_file);
}

/** \brief open file and prepare to write items.
 *
 * this method writes the file header and prepares to write items.
 * \return true for success, false for error.
 */

bool file_writer::open_file(void)
{
    m_file = fopen(m_filename, "wb");
    if (m_file == NULL) {
        perror(m_filename);
        return false;
    }

    fprintf(m_file, "dumpflags, time, exptime, nbytes, nsuffix, it_flags, clsid, nkey, key, data\n");
    return true;
}

/** \brief return a quoted string which is suitable for printing into
 * a csv format.
 *
 * quoting is done like this: every double-quote (") character is replaced
 * with two double-quote ("") characters.
 *
 * \param str string to quote.
 * \param str_len length of str.
 * \param new_str_len pointer to int which will be set with the new, quoted string length.
 * \return pointer to quoted string, or to the same string if quoting is not needed.
 */

char *file_writer::get_quoted_str(char *str, int str_len, int *new_str_len)
{
    char *new_str;
    char *d;
    char *s;

    *new_str_len = str_len;

    // is it necessary?
    if (memchr(str, '"', str_len) == NULL) {
        return str;
    }

    // assume worst case for memory allocation
    new_str = (char *) malloc(str_len * 2);
    if (!new_str) {
        fprintf(stderr, "get_quoted_str: error: out of memory\n");
        return str;
    }

    // copy & quote
    for (s = str, d = new_str; s - str < str_len; s++, d++) {
        *d = *s;
        if (*s == '"') {
            d++;
            *d = '"';
        }
    }

    *new_str_len = d - new_str;
    return new_str;
}

/** \brief write an item to an open file.
 * \param item pointer to memcache_item object to write.
 * \return true for success, false for error.
 */
bool file_writer::write_item(memcache_item *item)
{
    char *quoted_key;
    int quoted_key_len;
    char *quoted_data;
    int quoted_data_len;

    quoted_key = get_quoted_str(item->get_key(), item->get_nkey(), &quoted_key_len);
    quoted_data = get_quoted_str(item->get_data(), item->get_nbytes() - 2, &quoted_data_len);

    fprintf(m_file, "%u, %u, %u, %u, %u, %u, %u, %u, \"%.*s\", \"", item->get_dumpflags(),
            (unsigned int) item->get_time(), (unsigned int) item->get_exptime(), item->get_nbytes(),
            item->get_nsuffix(), item->get_flags(), item->get_clsid(), item->get_nkey(), quoted_key_len, quoted_key);
    if (fwrite(quoted_data, quoted_data_len, 1, m_file) != 1) {
        perror(m_filename);
        return false;
    }
    if (fputs("\"\n", m_file) == EOF) {
        perror(m_filename);
        return false;
    }

    if (quoted_key != item->get_key()) free(quoted_key);
    if (quoted_data != item->get_data()) free(quoted_data);

    return true;
}
