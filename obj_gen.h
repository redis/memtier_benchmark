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

#ifndef _OBJ_GEN_H
#define _OBJ_GEN_H

#include <vector>
#include "file_io.h"

struct random_data;
struct config_weight_list;

class random_generator {
public:
    random_generator();
    unsigned long long get_random();
    unsigned long long get_random_max() const;
    void set_seed(int seed);
private:
#ifdef HAVE_RANDOM_R
    struct random_data m_data_blob;
    char m_state_array[512];
#elif (defined HAVE_DRAND48)
    unsigned short m_data_blob[3];
#endif
};

class gaussian_noise: public random_generator {
public:
    gaussian_noise() { m_hasSpare = false; }
    unsigned long long gaussian_distribution_range(double stddev, double median, unsigned long long min, unsigned long long max);
private:
    double gaussian_distribution(const double &stddev);
    bool m_hasSpare;
    double m_spare;
};

#define OBJECT_GENERATOR_KEY_ITERATORS  2 /* number of iterators */
#define OBJECT_GENERATOR_KEY_SET_ITER   1
#define OBJECT_GENERATOR_KEY_GET_ITER   0
#define OBJECT_GENERATOR_KEY_RANDOM    -1
#define OBJECT_GENERATOR_KEY_GAUSSIAN  -2
#define OBJECT_GENERATOR_KEY_ZIPFIAN   -3

class object_generator {
public:
    enum data_size_type { data_size_unknown, data_size_fixed, data_size_range, data_size_weighted };
protected:
    data_size_type m_data_size_type;
    union {
        unsigned int size_fixed;
        struct {
            unsigned int size_min;
            unsigned int size_max;
        } size_range;
        config_weight_list* size_list;
    } m_data_size;
    const char *m_data_size_pattern;
    bool m_random_data;
    unsigned int m_expiry_min;
    unsigned int m_expiry_max;
    const char *m_key_prefix;
    unsigned long long m_key_min;
    unsigned long long m_key_max;
    double m_key_stddev;
    double m_key_median;

    // zipf will only be used for key generation
    // adjusted min and max key for zipf, may be difference from user specified
    unsigned long long m_key_zipf_min;
    unsigned long long m_key_zipf_max;
    // other persist data across generations
    double m_key_zipf_exp;
    double m_key_zipf_1mexp;
    double m_key_zipf_1mexpInv;
    double m_key_zipf_Hmin;
    double m_key_zipf_Hmax;
    double m_key_zipf_s;

    std::vector<unsigned long long> m_next_key;

    unsigned long long m_key_index;
    char m_key_buffer[250];
    const char *m_key;
    int m_key_len;
    char *m_value_buffer;
    int m_random_fd;
    gaussian_noise m_random;
    unsigned int m_value_buffer_size;
    unsigned int m_value_buffer_mutation_pos;

    // Bulk key generation support
    bool m_use_bulk_key_format;
    unsigned int m_bulk_slots;
    unsigned int m_bulk_size;
    unsigned long long m_current_command_count;
    unsigned long long m_initial_slot_id;
    unsigned long long m_initial_key_suffix;
    int m_key_suffix_width;
    unsigned int m_key_slot_part_size;  // Size to pad the slot_id part inside {}
    unsigned int m_key_rest_size;       // Size to pad the key_suffix part after {}

    void alloc_value_buffer(void);
    void alloc_value_buffer(const char* copy_from);
    void random_init(void);
public:
    object_generator(size_t n_key_iterators = OBJECT_GENERATOR_KEY_ITERATORS);
    object_generator(const object_generator& copy);
    virtual ~object_generator();
    virtual object_generator* clone(void);

    unsigned long long random_range(unsigned long long r_min, unsigned long long r_max);
    unsigned long long normal_distribution(unsigned long long r_min, unsigned long long r_max, double r_stddev, double r_median);
    unsigned long long zipf_distribution();

    void set_random_data(bool random_data);
    void set_data_size_fixed(unsigned int size);
    void set_data_size_range(unsigned int size_min, unsigned int size_max);
    void set_data_size_list(config_weight_list* data_size_list);
    void set_data_size_pattern(const char* pattern);
    void set_expiry_range(unsigned int expiry_min, unsigned int expiry_max);
    void set_key_prefix(const char *key_prefix);
    void set_key_range(unsigned long long key_min, unsigned long long key_max);
    void set_key_distribution(double key_stddev, double key_median);
    void set_key_zipf_distribution(double key_exp);
    void set_random_seed(int seed);
    void set_bulk_slots(unsigned int bulk_slots);
    void set_bulk_size(unsigned int bulk_size);
    void set_use_bulk_key_format(bool use_bulk_key_format);
    bool set_key_slot_part_size(unsigned int size);
    bool set_key_rest_size(unsigned int size);
    void randomize_bulk_initial_values();
    unsigned long long get_key_index(int iter);
    void generate_key(unsigned long long key_index);
    const char * get_key() { return m_key; }
    int get_key_len() { return m_key_len; }

    const char * get_key_prefix();
    virtual const char* get_value(unsigned long long key_index, unsigned int *len);
    virtual unsigned int get_expiry();
};

class imported_keylist;
class memcache_item;

class imported_keylist {
protected:
    struct key {
        unsigned int key_len;
        char key_data[0];
    };
    const char *m_filename;
    std::vector<key*> m_keys;
public:
    imported_keylist(const char *filename);
    ~imported_keylist();

    bool read_keys(void);
    unsigned int size();
    const char* get(unsigned int pos, unsigned int *len);
};

class import_object_generator : public object_generator {
protected:
    imported_keylist* m_keys;
    file_reader m_reader;
    memcache_item* m_cur_item;
    bool m_reader_opened;
    bool m_no_expiry;
public:
    import_object_generator(const char *filename, imported_keylist* keys, bool no_expiry);
    import_object_generator(const import_object_generator& from);
    virtual ~import_object_generator();
    virtual import_object_generator* clone(void);

    void read_next_item(void);
    void read_next_key(unsigned long long key_index);

    virtual const char* get_value(unsigned long long key_index, unsigned int *len);
    virtual unsigned int get_expiry();

    bool open_file(void);
};

#endif /* _OBJ_GEN_H */
