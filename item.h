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

#ifndef _ITEM_H
#define _ITEM_H

#include <sys/time.h>

#include <stdlib.h>

/** \name values of the bitwise dump flag */
#define ITEM_DUMPFLAGS_EXPIRED 0x0001 /** item's expiration time has passed when dump conducted */

/** represents a memcache item while it's being processed by the tool. */
class memcache_item
{
protected:
    unsigned int m_dumpflags;    /** from file: dump flags (added by memcache_dump) */
    time_t m_time;               /** from file: time (last modified time) */
    time_t m_exptime;            /** from file: exptime (expiration time) */
    unsigned int m_nbytes;       /** from file: nbytes (size of data, including trailing CRLF) */
    unsigned int m_nkey;         /** from file: nkey (size of key) */
    unsigned short m_flags;      /** from file: flags (internal memcached) */
    unsigned int m_nsuffix;      /** from file: suffix length */
    unsigned int m_clsid;        /** from file: clsid */
    char *m_key;                 /** item's key */
    char *m_data;                /** item's data */
    unsigned long int m_version; /** item version, as determined by PCRE regex */
public:
    memcache_item(unsigned int dumpflags, time_t time, time_t exptime, unsigned short flags, unsigned int nsuffix,
                  unsigned int clsid);
    void set_key(char *key, unsigned int nkey);
    void set_data(char *data, unsigned int nbytes);
    ~memcache_item();

    char *get_key(void);
    unsigned int get_nkey(void);

    char *get_data(void);
    unsigned int get_nbytes(void);

    time_t get_time(void);
    time_t get_exptime(void);
    unsigned int get_dumpflags(void);
    unsigned short get_flags(void);
    unsigned int get_nsuffix(void);
    unsigned int get_clsid(void);

    bool is_expired(void);

    void set_version(unsigned long int version);
    unsigned long int get_version(void);

    int operator<(const memcache_item &a);
};

extern int memcache_item_ptr_cmp(memcache_item *a, memcache_item *b);

#endif /* _ITEM_H */
