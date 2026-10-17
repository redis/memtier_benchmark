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

#include "item.h"

memcache_item::memcache_item(unsigned int dumpflags, time_t time, time_t exptime, unsigned short flags,
                             unsigned int nsuffix, unsigned int clsid) :
        m_dumpflags(dumpflags),
        m_time(time),
        m_exptime(exptime),
        m_flags(flags),
        m_nsuffix(nsuffix),
        m_clsid(clsid),
        m_version(0)
{
    m_key = NULL;
    m_data = NULL;
}

memcache_item::~memcache_item()
{
    if (m_key != NULL) free(m_key);
    if (m_data != NULL) free(m_data);
}

/** \brief set a memcache_item's key.
 * \param key pointer to malloc() allocated key data.
 * \param nkey length of key.
 */

void memcache_item::set_key(char *key, unsigned int nkey)
{
    if (m_key != NULL) free(m_key);
    m_nkey = nkey;
    m_key = key;
}

/** \brief set a memcache_item's data.
 * \param data pointer to malloc() allocated data.
 * \param nbytes length of data.
 */

void memcache_item::set_data(char *data, unsigned int nbytes)
{
    if (m_data != NULL) free(m_data);
    m_nbytes = nbytes;
    m_data = data;
}

char *memcache_item::get_key(void)
{
    return m_key;
}

unsigned int memcache_item::get_nkey(void)
{
    return m_nkey;
}

char *memcache_item::get_data(void)
{
    return m_data;
}

unsigned int memcache_item::get_nbytes(void)
{
    return m_nbytes;
}

void memcache_item::set_version(unsigned long int version)
{
    m_version = version;
}

unsigned long int memcache_item::get_version(void)
{
    return m_version;
}

time_t memcache_item::get_time(void)
{
    return m_time;
}

time_t memcache_item::get_exptime(void)
{
    return m_exptime;
}

unsigned int memcache_item::get_dumpflags(void)
{
    return m_dumpflags;
}

unsigned short memcache_item::get_flags(void)
{
    return m_flags;
}

unsigned int memcache_item::get_nsuffix(void)
{
    return m_nsuffix;
}

unsigned int memcache_item::get_clsid(void)
{
    return m_clsid;
}

bool memcache_item::is_expired(void)
{
    return ((m_dumpflags & ITEM_DUMPFLAGS_EXPIRED) == ITEM_DUMPFLAGS_EXPIRED);
}

int memcache_item::operator<(const memcache_item &a)
{
    if (this->m_time < a.m_time)
        return 1;
    else
        return 0;
}

int memcache_item_ptr_cmp(memcache_item *a, memcache_item *b)
{
    return (*a < *b);
}
