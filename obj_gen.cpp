/*
 * Copyright (C) 2011-2017 Redis Labs Ltd.
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <math.h>

#ifdef HAVE_ASSERT_H
#include <assert.h>
#endif

#include "obj_gen.h"
#include "memtier_benchmark.h"

random_generator::random_generator()
{
    set_seed(0);
}

void random_generator::set_seed(int seed)
{
    seed++; //http://stackoverflow.com/questions/27386470/srand0-and-srand1-give-the-same-results
#ifdef HAVE_RANDOM_R
    memset(&m_data_blob, 0, sizeof(m_data_blob));
    memset(m_state_array, 0, sizeof(m_state_array));

    int ret = initstate_r(seed, m_state_array, sizeof(m_state_array), &m_data_blob);
    assert(ret == 0);
#elif (defined HAVE_DRAND48)
    memset(&m_data_blob, 0, sizeof(m_data_blob));
    size_t seed_size = sizeof(seed); //get MIN size between seed and m_data_blob
    if (seed_size > sizeof(m_data_blob))
        seed_size = sizeof(m_data_blob);
    memcpy(&m_data_blob, &seed, seed_size);
#endif
}

unsigned long long random_generator::get_random()
{
    unsigned long long llrn;
#ifdef HAVE_RANDOM_R
    int32_t rn;
    // max is RAND_MAX, which is usually 2^31-1 (although can be as low as 2^16-1, which we ignore now)
    // this is fine, especially considering that random_r is a nonstandard glibc extension
    // it returns a positive int32_t, so either way the MSB is off
    int ret = random_r(&m_data_blob, &rn);
    assert(ret == 0);
    llrn = rn;
    llrn = llrn << 31;

    ret = random_r(&m_data_blob, &rn);
    assert(ret == 0);
    llrn |= rn;
#elif (defined HAVE_DRAND48)
    long rn;
    // jrand48's range is -2^31..+2^31 (i.e. all 32 bits)
    rn = jrand48(m_data_blob);
    llrn = rn;
    llrn = llrn << 32;

    rn = jrand48(m_data_blob);
    llrn |= rn & 0xffffffff; // reset the sign extension bits of negative numbers
    llrn &= 0x7FFFFFFFFFFFFFFF; // avoid any trouble from sign mismatch and negative numbers
#else
    #error no random function
#endif
    return llrn;
}

unsigned long long random_generator::get_random_max() const
{
#ifdef HAVE_RANDOM_R
    return 0x3fffffffffffffff;//62 bits
#elif (defined HAVE_DRAND48)
    return 0x7fffffffffffffff;//63 bits
#endif
}

//returns a value surrounding 0
double gaussian_noise::gaussian_distribution(const double &stddev)
{
    // Box–Muller transform (Marsaglia polar method)
    if (m_hasSpare) {
        m_hasSpare = false;
        return stddev * m_spare;
    }

    m_hasSpare = true;
    double u, v, s;
    do {
        u = (get_random() / ((double) get_random_max())) * 2 - 1;
        v = (get_random() / ((double) get_random_max())) * 2 - 1;
        s = u * u + v * v;
    } while(s >= 1 || s == 0);

    s = sqrt(-2.0 * log(s) / s);
    m_spare = v * s;
    return stddev * u * s;
}

unsigned long long gaussian_noise::gaussian_distribution_range(double stddev, double median, unsigned long long min, unsigned long long max)
{
    if (min==max)
        return min;

    unsigned long long len = max-min;

    double val;
    if (median == 0)
        median = len / 2.0 + min + 0.5;
    if (stddev == 0)
        stddev = len / 6.0;
    assert(median > min && median < max);
    do {
        val = gaussian_distribution(stddev) + median;
    } while(val < min || val > max + 1);
    return val;
}

object_generator::object_generator(size_t n_key_iterators/*= OBJECT_GENERATOR_KEY_ITERATORS*/) :
    m_data_size_type(data_size_unknown),
    m_data_size_pattern(NULL),
    m_random_data(false),
    m_expiry_min(0),
    m_expiry_max(0),
    m_key_prefix(NULL),
    m_key_min(0),
    m_key_max(0),
    m_key_stddev(0),
    m_key_median(0),
    m_key_zipf_min(0),
    m_key_zipf_max(0),
    m_key_zipf_exp(1),
    m_key_zipf_1mexp(0),
    m_key_zipf_1mexpInv(0),
    m_key_zipf_Hmin(0),
    m_key_zipf_Hmax(0),
    m_key_zipf_s(0),
    m_value_buffer(NULL),
    m_random_fd(-1),
    m_value_buffer_size(0),
    m_value_buffer_mutation_pos(0),
    m_use_bulk_key_format(false),
    m_bulk_slots(0),
    m_bulk_size(0),
    m_current_command_count(0),
    m_initial_slot_id(0),
    m_initial_key_suffix(0),
    m_key_suffix_width(0),
    m_key_slot_part_size(0),
    m_key_rest_size(0)
{
    m_next_key.resize(n_key_iterators, 0);

    m_data_size.size_list = NULL;
}

object_generator::object_generator(const object_generator& copy) :
    m_data_size_type(copy.m_data_size_type),
    m_data_size(copy.m_data_size),
    m_data_size_pattern(copy.m_data_size_pattern),
    m_random_data(copy.m_random_data),
    m_expiry_min(copy.m_expiry_min),
    m_expiry_max(copy.m_expiry_max),
    m_key_prefix(copy.m_key_prefix),
    m_key_min(copy.m_key_min),
    m_key_max(copy.m_key_max),
    m_key_stddev(copy.m_key_stddev),
    m_key_median(copy.m_key_median),
    m_key_zipf_min(copy.m_key_zipf_min),
    m_key_zipf_max(copy.m_key_zipf_max),
    m_key_zipf_exp(copy.m_key_zipf_exp),
    m_key_zipf_1mexp(copy.m_key_zipf_1mexp),
    m_key_zipf_1mexpInv(copy.m_key_zipf_1mexpInv),
    m_key_zipf_Hmin(copy.m_key_zipf_Hmin),
    m_key_zipf_Hmax(copy.m_key_zipf_Hmax),
    m_key_zipf_s(copy.m_key_zipf_s),
    m_value_buffer(NULL),
    m_random_fd(-1),
    m_value_buffer_size(0),
    m_value_buffer_mutation_pos(0),
    m_use_bulk_key_format(copy.m_use_bulk_key_format),
    m_bulk_slots(copy.m_bulk_slots),
    m_bulk_size(copy.m_bulk_size),
    m_current_command_count(0),
    m_initial_slot_id(0),
    m_initial_key_suffix(0),
    m_key_suffix_width(copy.m_key_suffix_width),
    m_key_slot_part_size(copy.m_key_slot_part_size),
    m_key_rest_size(copy.m_key_rest_size)
{
    if (m_data_size_type == data_size_weighted &&
        m_data_size.size_list != NULL) {
        m_data_size.size_list = new config_weight_list(*m_data_size.size_list);
    }
    alloc_value_buffer(copy.m_value_buffer);

    m_next_key.resize(copy.m_next_key.size(), 0);

    // Note: Random initial values for bulk format are NOT generated here.
    // They will be generated later via randomize_bulk_initial_values() after set_random_seed()
    // is called, ensuring each client gets different random values.
}

object_generator::~object_generator()
{
    if (m_value_buffer != NULL)
        free(m_value_buffer);
    if (m_data_size_type == data_size_weighted &&
        m_data_size.size_list != NULL) {
        delete m_data_size.size_list;
    }
    if (m_random_fd != -1) {
        close(m_random_fd);
        m_random_fd = -1;
    }
}

object_generator* object_generator::clone(void)
{
    return new object_generator(*this);
}

void object_generator::set_random_seed(int seed)
{
    m_random.set_seed(seed);
}

void object_generator::alloc_value_buffer(void)
{
    unsigned int size = 0;

    if (m_value_buffer != NULL)
        free(m_value_buffer), m_value_buffer = NULL;

    if (m_data_size_type == data_size_fixed)
        size = m_data_size.size_fixed;
    else if (m_data_size_type == data_size_range)
        size = m_data_size.size_range.size_max;
    else if (m_data_size_type == data_size_weighted) {
        size = m_data_size.size_list->largest();
    }

    m_value_buffer_size = size;
    if (size > 0) {
        m_value_buffer = (char*) malloc(size);
        assert(m_value_buffer != NULL);
        if (!m_random_data) {
            memset(m_value_buffer, 'x', size);
        } else {
            if (m_random_fd == -1) {
                m_random_fd = open("/dev/urandom",  O_RDONLY);
                assert(m_random_fd != -1);
            }

            char buf1[64] = { 0 };
            char buf2[64] = { 0 };
            unsigned int buf1_idx = sizeof(buf1);
            unsigned int buf2_idx = sizeof(buf2);
            char *d = m_value_buffer;
            int ret;
            int iter = 0;
            while (d - m_value_buffer < size) {
                if (buf1_idx == sizeof(buf1)) {
                    buf1_idx = 0;
                    buf2_idx++;

                    if (buf2_idx >= sizeof(buf2)) {
                        if (iter % 20 == 0) {
                            ret = read(m_random_fd, buf1, sizeof(buf1));
                            assert(ret > -1);
                            ret = read(m_random_fd, buf2, sizeof(buf2));
                            assert(ret > -1);
                        }
                        buf2_idx = 0;
                        iter++;
                    }
                }
                *d = buf1[buf1_idx] ^ buf2[buf2_idx] ^ iter;
                d++;
                buf1_idx++;
            }
        }
    }
}

void object_generator::alloc_value_buffer(const char* copy_from)
{
    unsigned int size = 0;

    if (m_value_buffer != NULL)
        free(m_value_buffer), m_value_buffer = NULL;

    if (m_data_size_type == data_size_fixed)
        size = m_data_size.size_fixed;
    else if (m_data_size_type == data_size_range)
        size = m_data_size.size_range.size_max;
    else if (m_data_size_type == data_size_weighted)
        size = m_data_size.size_list->largest();

    m_value_buffer_size = size;
    if (size > 0) {
        m_value_buffer = (char*) malloc(size);
        assert(m_value_buffer != NULL);
        memcpy(m_value_buffer, copy_from, size);
    }
}

void object_generator::set_random_data(bool random_data)
{
    m_random_data = random_data;
}

void object_generator::set_data_size_fixed(unsigned int size)
{
    m_data_size_type = data_size_fixed;
    m_data_size.size_fixed = size;
    alloc_value_buffer();
}

void object_generator::set_data_size_range(unsigned int size_min, unsigned int size_max)
{
    m_data_size_type = data_size_range;
    m_data_size.size_range.size_min = size_min;
    m_data_size.size_range.size_max = size_max;
    alloc_value_buffer();
}

void object_generator::set_data_size_list(config_weight_list* size_list)
{
    if (m_data_size_type == data_size_weighted && m_data_size.size_list != NULL) {
        delete m_data_size.size_list;
    }
    m_data_size_type = data_size_weighted;
    m_data_size.size_list = new config_weight_list(*size_list);
    alloc_value_buffer();
}

void object_generator::set_data_size_pattern(const char* pattern)
{
    m_data_size_pattern = pattern;
}

void object_generator::set_expiry_range(unsigned int expiry_min, unsigned int expiry_max)
{
    m_expiry_min = expiry_min;
    m_expiry_max = expiry_max;
}

void object_generator::set_key_prefix(const char *key_prefix)
{
    m_key_prefix = key_prefix;
}

void object_generator::set_key_range(unsigned long long key_min, unsigned long long key_max)
{
    m_key_min = key_min;
    m_key_max = key_max;
}

void object_generator::set_key_distribution(double key_stddev, double key_median)
{
    m_key_stddev = key_stddev;
    m_key_median = key_median;
}

// should be called after set_key_range in memtier_benchmark.cpp
void object_generator::set_key_zipf_distribution(double key_exp)
{
    const double eps = 1e-4;

    if (key_exp < eps)
        m_key_zipf_exp = 0.;
    else if (fabs(key_exp - 1) < eps)
        m_key_zipf_exp = 1.;
    else
        m_key_zipf_exp = key_exp;

    if (m_key_min == 0)
        m_key_zipf_min = 1;
    else
        m_key_zipf_min = m_key_min;

    if (m_key_max <= m_key_zipf_min)
        m_key_zipf_max = m_key_zipf_min;
    else
        m_key_zipf_max = m_key_max;

    if (m_key_zipf_exp < eps)
        return; // degenerated to uniform distribution
    else if (fabs(key_exp - 1) < eps) {
        m_key_zipf_Hmin = log(m_key_zipf_min + 0.5) - 1. / m_key_zipf_min;
        m_key_zipf_Hmax = log(m_key_zipf_max + 0.5);
        double t = log(m_key_zipf_min + 1.5) - 1. / (m_key_zipf_min + 1);
        m_key_zipf_s = m_key_zipf_min + 1 - exp(t);
    } else {
        m_key_zipf_1mexp = 1. - m_key_zipf_exp;
        m_key_zipf_1mexpInv = 1. / m_key_zipf_1mexp;
        m_key_zipf_Hmin = pow(m_key_zipf_min + 0.5, m_key_zipf_1mexp) - 
            m_key_zipf_1mexp * pow(m_key_zipf_min, -m_key_zipf_exp);
        m_key_zipf_Hmax = pow(m_key_zipf_max + 0.5, m_key_zipf_1mexp);
        double t = pow(m_key_zipf_min + 1.5, m_key_zipf_1mexp) - 
            m_key_zipf_1mexp * pow(m_key_zipf_min + 1, -m_key_zipf_exp);
        m_key_zipf_s = m_key_zipf_min + 1 - pow(t, m_key_zipf_1mexpInv);
    }
}

void object_generator::set_bulk_slots(unsigned int bulk_slots)
{
    m_bulk_slots = bulk_slots;

    // Calculate key_suffix width based on keys_per_slot
    if (bulk_slots > 0 && m_key_max >= m_key_min) {
        unsigned long long total_keys = m_key_max - m_key_min + 1;
        unsigned long long keys_per_slot = total_keys / bulk_slots;

        // Calculate width needed to represent (keys_per_slot - 1)
        m_key_suffix_width = 0;
        unsigned long long temp = keys_per_slot - 1;
        while (temp > 0) {
            m_key_suffix_width++;
            temp /= 10;
        }
        if (m_key_suffix_width == 0)
            m_key_suffix_width = 1;
    }
}

void object_generator::set_bulk_size(unsigned int bulk_size)
{
    m_bulk_size = bulk_size;
}

void object_generator::set_use_bulk_key_format(bool use_bulk_key_format)
{
    m_use_bulk_key_format = use_bulk_key_format;

    // Initialize random starting values if bulk format is enabled
    if (use_bulk_key_format && m_bulk_slots > 0) {
        m_initial_slot_id = random_range(0, m_bulk_slots - 1);
        unsigned long long keys_per_slot = (m_key_max - m_key_min + 1) / m_bulk_slots;
        if (keys_per_slot > 0) {
            m_initial_key_suffix = random_range(0, keys_per_slot - 1);
        }
    }
}

bool object_generator::set_key_slot_part_size(unsigned int size)
{
    if (size == 0) {
        m_key_slot_part_size = 0;
        return true;
    }

    // Validate that size is large enough for the maximum slot_id
    if (m_bulk_slots > 0) {
        unsigned long long max_slot_id = m_bulk_slots - 1;

        // Count digits in max_slot_id
        int num_digits = 0;
        unsigned long long temp = max_slot_id;
        if (temp == 0) {
            num_digits = 1;
        } else {
            while (temp > 0) {
                num_digits++;
                temp /= 10;
            }
        }

        if (size < (unsigned int)num_digits) {
            return false;
        }
    }

    m_key_slot_part_size = size;
    return true;
}

bool object_generator::set_key_rest_size(unsigned int size)
{
    if (size == 0) {
        m_key_rest_size = 0;
        return true;
    }

    // Validate that size is large enough for the maximum key_suffix
    if (m_bulk_slots > 0 && m_key_max > m_key_min) {
        // Count digits in max_key_suffix (considering zero-padding width)
        int num_digits = m_key_suffix_width;

        if (size < (unsigned int)num_digits) {
            return false;
        }
    }

    m_key_rest_size = size;
    return true;
}

void object_generator::randomize_bulk_initial_values()
{
    // Generate random initial values for bulk key format
    // This should be called after set_random_seed() to ensure different clients get different values
    if (m_use_bulk_key_format && m_bulk_slots > 0) {
        m_initial_slot_id = random_range(0, m_bulk_slots - 1);
        unsigned long long keys_per_slot = (m_key_max - m_key_min + 1) / m_bulk_slots;
        if (keys_per_slot > 0) {
            m_initial_key_suffix = random_range(0, keys_per_slot - 1);
        }
    }
}

// return a random number between r_min and r_max
unsigned long long object_generator::random_range(unsigned long long r_min, unsigned long long  r_max)
{
    unsigned long long rn = m_random.get_random();
    return (rn % (r_max - r_min + 1)) + r_min;
}

// return a random number between r_min and r_max using normal distribution according to r_stddev
unsigned long long object_generator::normal_distribution(unsigned long long r_min, unsigned long long r_max, double r_stddev, double r_median)
{
    return m_random.gaussian_distribution_range(r_stddev, r_median, r_min, r_max);
}

// following sampler is based on:
// Rejection-inversion to generate variates from monotone discrete distributions
// ACM Transactions on Modeling and Computer Simulation.
// Volume 6 Issue 3 July 1996 pp 169–184
// https://doi.org/10.1145/235025.235029
unsigned long long object_generator::zipf_distribution()
{
    const double eps = 1e-4;

    if (m_key_zipf_exp < eps)
        return random_range(m_key_zipf_min, m_key_zipf_max);
    else if (fabs(m_key_zipf_exp - 1.0) < eps) {
        while (true) {
            double p = m_random.get_random() / (double)(m_random.get_random_max());
            double u = p * (m_key_zipf_Hmax - m_key_zipf_Hmin) + m_key_zipf_Hmin;
            double x = exp(u);
            if (x < m_key_zipf_min - 0.5)
                x = m_key_zipf_min + 0.5;
            if (x >= m_key_zipf_max + 0.5)
                x = m_key_zipf_max;
            double k = floor(x + 0.5);
            if (k - x <= m_key_zipf_s)
                return k;
            if (u > log(k + 0.5) - 1. / k)
                return k;
        }
    } else {
        while (true) {
            double p = m_random.get_random() / (double)(m_random.get_random_max());
            double u = p * (m_key_zipf_Hmax - m_key_zipf_Hmin) + m_key_zipf_Hmin;
            double x = pow(u, m_key_zipf_1mexpInv);
            if (x < m_key_zipf_min - 0.5)
                x = m_key_zipf_min + 0.5;
            if (x >= m_key_zipf_max + 0.5)
                x = m_key_zipf_max;
            double k = floor(x + 0.5);
            if (k - x <= m_key_zipf_s)
                return k;
            double t = (u - pow(k + 0.5, m_key_zipf_1mexp));
            if (m_key_zipf_1mexpInv * t > -pow(k, -m_key_zipf_exp))
                return k;
        }
    }
}

unsigned long long object_generator::get_key_index(int iter)
{
    assert(iter < static_cast<int>(m_next_key.size()) && iter >= OBJECT_GENERATOR_KEY_ZIPFIAN);

    unsigned long long k;
    if (iter==OBJECT_GENERATOR_KEY_RANDOM) {
        k = random_range(m_key_min, m_key_max);
    } else if(iter==OBJECT_GENERATOR_KEY_GAUSSIAN) {
        k = normal_distribution(m_key_min, m_key_max, m_key_stddev, m_key_median);
    } else if(iter == OBJECT_GENERATOR_KEY_ZIPFIAN) {
        k = zipf_distribution();
    } else {
        if (m_next_key[iter] < m_key_min)
            m_next_key[iter] = m_key_min;
        k = m_next_key[iter];

        m_next_key[iter]++;
        if (m_next_key[iter] > m_key_max)
            m_next_key[iter] = m_key_min;
    }
    return k;
}

void object_generator::generate_key(unsigned long long key_index) {
    if (m_use_bulk_key_format && m_bulk_slots > 0) {
        // Bulk key generation: {slot_id}:key_suffix with optional padding
        unsigned long long total_keys = m_key_max - m_key_min + 1;
        unsigned long long keys_per_slot = total_keys / m_bulk_slots;

        // Calculate current bulk number and key_suffix
        unsigned long long bulk_number = m_current_command_count / m_bulk_size;
        unsigned long long key_suffix = (m_initial_key_suffix + m_current_command_count) % keys_per_slot;
        unsigned long long slot_id = (m_initial_slot_id + bulk_number) % m_bulk_slots;

        // Build the key with optional padding - optimized version
        char *buf = m_key_buffer;
        char *buf_end = m_key_buffer + sizeof(m_key_buffer) - 1;

        // Start with opening brace
        *buf++ = '{';

        // Format slot_id part with optional left padding using 'x'
        if (m_key_slot_part_size > 0) {
            // Format number to temporary buffer
            char num_str[32];
            int num_len = snprintf(num_str, sizeof(num_str), "%llu", slot_id);

            // Pad with 'x' on the left
            int padding = m_key_slot_part_size - num_len;
            if (padding > 0) {
                memset(buf, 'x', padding);
                buf += padding;
                memcpy(buf, num_str, num_len);
                buf += num_len;
            } else {
                memcpy(buf, num_str, num_len);
                buf += num_len;
            }
        } else {
            // No padding - format directly to buffer
            buf += snprintf(buf, buf_end - buf, "%llu", slot_id);
        }

        // Add closing brace and colon
        *buf++ = '}';
        *buf++ = ':';

        // Format key_suffix part with optional left padding using 'x'
        if (m_key_rest_size > 0) {
            // Format number with zero-padding to temporary buffer
            char num_str[32];
            int num_len = snprintf(num_str, sizeof(num_str), "%0*llu", m_key_suffix_width, key_suffix);

            // Pad with 'x' on the left
            int padding = m_key_rest_size - num_len;
            if (padding > 0) {
                memset(buf, 'x', padding);
                buf += padding;
                memcpy(buf, num_str, num_len);
                buf += num_len;
            } else {
                memcpy(buf, num_str, num_len);
                buf += num_len;
            }
        } else {
            // No padding - format directly to buffer
            buf += snprintf(buf, buf_end - buf, "%0*llu", m_key_suffix_width, key_suffix);
        }

        // Null terminate and calculate length
        *buf = '\0';
        m_key_len = buf - m_key_buffer;
        m_key = m_key_buffer;

        // Increment command count for next key
        m_current_command_count++;
    } else {
        // Standard key generation: key_prefix + key_index
        m_key_len = snprintf(m_key_buffer, sizeof(m_key_buffer)-1, "%s%llu", m_key_prefix, key_index);
        m_key = m_key_buffer;
    }
}

const char* object_generator::get_key_prefix() {
    return m_key_prefix;
}

const char* object_generator::get_value(unsigned long long key_index, unsigned int *len) {
    // compute size
    unsigned int new_size = 0;
    if (m_data_size_type == data_size_fixed) {
        new_size = m_data_size.size_fixed;
    } else if (m_data_size_type == data_size_range) {
        if (m_data_size_pattern && *m_data_size_pattern=='S') {
            double a = (key_index-m_key_min)/static_cast<double>(m_key_max-m_key_min);
            new_size = (m_data_size.size_range.size_max-m_data_size.size_range.size_min)*a + m_data_size.size_range.size_min;
        } else {
            new_size = random_range(m_data_size.size_range.size_min > 0 ? m_data_size.size_range.size_min : 1,
                                    m_data_size.size_range.size_max);
        }
    } else if (m_data_size_type == data_size_weighted) {
        new_size = m_data_size.size_list->get_next_size();
    } else {
        assert(0);
    }

    // modify object content in case of random data
    if (m_random_data) {
        m_value_buffer[m_value_buffer_mutation_pos++]++;
        if (m_value_buffer_mutation_pos >= m_value_buffer_size)
            m_value_buffer_mutation_pos = 0;
    }

    *len = new_size;
    return m_value_buffer;
}

unsigned int object_generator::get_expiry() {
    // compute expiry
    unsigned int expiry = 0;
    if (m_expiry_max > 0) {
        expiry = random_range(m_expiry_min, m_expiry_max);
    }

    return expiry;
}

///////////////////////////////////////////////////////////////////////////

imported_keylist::imported_keylist(const char *filename)
    : m_filename(filename)
{
}

imported_keylist::~imported_keylist()
{
    for (unsigned int i = 0; i < m_keys.size(); i++) {
        free(m_keys[i]);
    }
    m_keys.clear();
}

bool imported_keylist::read_keys(void)
{
    file_reader f(m_filename);

    if (!f.open_file())
        return false;
    while (!f.is_eof()) {
        memcache_item *i = f.read_item();

        if (i != NULL) {
            key* k = (key*) malloc(i->get_nkey() + sizeof(key) + 1);
            assert(k != NULL);
            k->key_len = i->get_nkey();
            memcpy(k->key_data, i->get_key(), i->get_nkey());
            delete i;

            m_keys.push_back(k);
        }
    }

    return true;
}

unsigned int imported_keylist::size(void)
{
    return m_keys.size();
}

const char* imported_keylist::get(unsigned int pos, unsigned int *len)
{
    if (pos >= m_keys.size())
        return NULL;

    key* k = m_keys[pos];
    if (len != NULL) *len = k->key_len;

    return k->key_data;
}

///////////////////////////////////////////////////////////////////////////

import_object_generator::import_object_generator(const char *filename, imported_keylist *keys, bool no_expiry) :
    m_keys(keys),
    m_reader(filename),
    m_cur_item(NULL),
    m_reader_opened(false),
    m_no_expiry(no_expiry)
{
    if (m_keys != NULL) {
        m_key_max = m_keys->size();
        m_key_min = 1;
    }
}

import_object_generator::~import_object_generator()
{
    if (m_cur_item != NULL)
        delete m_cur_item;
}

import_object_generator::import_object_generator(const import_object_generator& from) :
    object_generator(from),
    m_keys(from.m_keys),
    m_reader(from.m_reader),
    m_cur_item(NULL),
    m_no_expiry(from.m_no_expiry)
{
    if (m_keys != NULL) {
        m_key_max = m_keys->size();
        m_key_min = 1;
    }
    if (from.m_reader_opened) {
        bool r = m_reader.open_file();
        assert(r == true);
    }
}

bool import_object_generator::open_file(void)
{
    m_reader_opened = true;
    return m_reader.open_file();
}

import_object_generator* import_object_generator::clone(void)
{
    return new import_object_generator(*this);
}

void import_object_generator::read_next_item() {
    /* Used by SET command to read an item that includes  KEY, VALUE, EXPIRE */
    memcache_item *i = m_reader.read_item();

    if (i == NULL && m_reader.is_eof()) {
        m_reader.open_file();
        i = m_reader.read_item();
    }

    assert(i != NULL);
    if (m_cur_item != NULL) {
        delete m_cur_item;
    }
    m_cur_item = i;

    m_key = m_cur_item->get_key();
    m_key_len = m_cur_item->get_nkey();
}

void import_object_generator::read_next_key(unsigned long long key_index) {
    /* Used by GET command that needs only a KEY */
    m_key = m_keys->get(key_index-1, (unsigned int *)&m_key_len);
}

const char* import_object_generator::get_value(unsigned long long key_index, unsigned int *len) {
    assert(m_cur_item != NULL);

    *len = m_cur_item->get_nbytes() - 2;
    return m_cur_item->get_data();
}

unsigned int import_object_generator::get_expiry() {
    assert(m_cur_item != NULL);

    // compute expiry
    unsigned int expiry = 0;
    if (!m_no_expiry) {
        if (m_expiry_max > 0) {
            expiry = random_range(m_expiry_min, m_expiry_max);
        } else {
            expiry = m_cur_item->get_exptime();
        }
    }

    return expiry;
}
