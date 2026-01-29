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

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <math.h>
#include <algorithm>

#ifdef HAVE_ASSERT_H
#include <assert.h>

#endif

#include "run_stats.h"

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

void output_table::add_column(table_column& col) {
    assert(columns.empty() || columns[0].elements.size() == col.elements.size());
    columns.push_back(col);
}

void output_table::print_header(FILE *out, const char * header) {
    if (header == NULL)
        return;

    fprintf(out, "\n\n");
    fprintf(out, "%s\n", header);

    for (unsigned int i=0; i<columns.size(); i++) {
        char buf[100];
        size_t header_size = columns[i].column_size + 1;
        memset(buf, '=', header_size);
        buf[header_size] = '\0';

        fprintf(out,"%s", buf);
    }

    fprintf(out,"\n");
}

void output_table::print(FILE *out, const char * header) {
    print_header(out, header);

    int num_of_elements = columns[0].elements.size();

    for (int i=0; i<num_of_elements; i++) {
        std::string line;
        char buf[100];

        for (unsigned int j=0; j<columns.size(); j++) {
            table_el* el = &columns[j].elements[i];
            switch (el->type) {
                case string_el:
                    snprintf(buf, 100, el->format.c_str(), el->str_value.c_str());
                    break;
                case double_el:
                    snprintf(buf, 100, el->format.c_str(), el->double_value);
                    break;
            }

            line += buf;
        }
        fprintf(out, "%s\n", line.c_str());
    }
}

///////////////////////////////////////////////////////////////////////////

inline unsigned long int ts_diff_now(struct timeval a)
{
    struct timeval b;

    gettimeofday(&b, NULL);
    unsigned long long aval = a.tv_sec * 1000000 + a.tv_usec;
    unsigned long long bval = b.tv_sec * 1000000 + b.tv_usec;

    return bval - aval;
}

inline timeval timeval_factorial_average(timeval a, timeval b, unsigned int weight)
{
    timeval tv;
    double factor = ((double)weight - 1) / weight;
    tv.tv_sec   = factor * a.tv_sec  + (double)b.tv_sec  / weight ;
    tv.tv_usec  = factor * a.tv_usec + (double)b.tv_usec / weight ;
    return (tv);
}

run_stats::run_stats(benchmark_config *config) :
           m_config(config),
           m_interrupted(false),
           m_totals(),
           m_cur_stats(0)
{
    memset(&m_start_time, 0, sizeof(m_start_time));
    memset(&m_end_time, 0, sizeof(m_end_time));
    std::vector<float> quantiles_list_float = config->print_percentiles.quantile_list;
    std::sort(quantiles_list_float.begin(), quantiles_list_float.end());
    quantiles_list = std::vector<double>(quantiles_list_float.begin(), quantiles_list_float.end());
    if (config->arbitrary_commands->is_defined()) {
        setup_arbitrary_commands(config->arbitrary_commands->size());
    }
}

void run_stats::setup_arbitrary_commands(size_t n_arbitrary_commands) {
    m_totals.setup_arbitrary_commands(n_arbitrary_commands);
    m_cur_stats.setup_arbitrary_commands(n_arbitrary_commands);
    m_ar_commands_latency_histograms.resize(n_arbitrary_commands);
    inst_m_ar_commands_latency_histograms.resize(n_arbitrary_commands);
}

void run_stats::set_start_time(struct timeval* start_time)
{
    struct timeval tv;
    if (!start_time) {
        gettimeofday(&tv, NULL);
        start_time = &tv;
    }

    m_start_time = *start_time;
}

void run_stats::set_end_time(struct timeval* end_time)
{
    struct timeval tv;
    if (!end_time) {
        gettimeofday(&tv, NULL);
        end_time = &tv;
    }
    m_end_time = *end_time;
    summarize_current_second();
    m_stats.push_back(m_cur_stats);
}

void run_stats::summarize_current_second(){
    m_cur_stats.m_get_cmd.summarize_quantiles(inst_m_get_latency_histogram,quantiles_list);
    m_cur_stats.m_set_cmd.summarize_quantiles(inst_m_set_latency_histogram,quantiles_list);
    m_cur_stats.m_wait_cmd.summarize_quantiles(inst_m_wait_latency_histogram,quantiles_list);
    m_cur_stats.m_total_cmd.summarize_quantiles(inst_m_totals_latency_histogram,quantiles_list);
    for (unsigned int i=0; i<m_cur_stats.m_ar_commands.size(); i++) {
        m_cur_stats.m_ar_commands[i].summarize_quantiles(inst_m_ar_commands_latency_histograms[i],quantiles_list);
    hdr_reset(inst_m_ar_commands_latency_histograms[i]);
    }
    hdr_reset(inst_m_get_latency_histogram);
    hdr_reset(inst_m_set_latency_histogram);
    hdr_reset(inst_m_wait_latency_histogram);
    hdr_reset(inst_m_totals_latency_histogram);
}

hdr_histogram* run_stats::get_inst_totals_histogram() const {
    return inst_m_totals_latency_histogram;
}

void run_stats::roll_cur_stats(struct timeval* ts)
{
    const unsigned int sec = ts_diff(m_start_time, *ts) / 1000000;
    if (sec > m_cur_stats.m_second) {
        summarize_current_second();
        m_stats.push_back(m_cur_stats);
        m_cur_stats.reset(sec);
    }
}

void run_stats::update_get_op(struct timeval* ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency, unsigned int hits, unsigned int misses)
{
    roll_cur_stats(ts);
    m_cur_stats.m_get_cmd.update_op(bytes_rx, bytes_tx, latency, hits, misses);
    m_cur_stats.m_total_cmd.update_op(bytes_rx, bytes_tx, latency, hits, misses);
    m_totals.update_op(bytes_rx, bytes_tx, latency);
    hdr_record_value_capped(m_get_latency_histogram,latency);
    hdr_record_value_capped(inst_m_get_latency_histogram,latency);
    hdr_record_value_capped(m_totals_latency_histogram,latency);
    hdr_record_value_capped(inst_m_totals_latency_histogram,latency);
}

void run_stats::update_set_op(struct timeval* ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency)
{
    roll_cur_stats(ts);

    m_cur_stats.m_set_cmd.update_op(bytes_rx, bytes_tx, latency);
    m_cur_stats.m_total_cmd.update_op(bytes_rx, bytes_tx, latency);
    m_totals.update_op(bytes_rx, bytes_tx, latency);
    hdr_record_value_capped(m_set_latency_histogram,latency);
    hdr_record_value_capped(inst_m_set_latency_histogram,latency);
    hdr_record_value_capped(m_totals_latency_histogram,latency);
    hdr_record_value_capped(inst_m_totals_latency_histogram,latency);
}

void run_stats::update_connection_error(struct timeval* ts)
{
    roll_cur_stats(ts);
    m_cur_stats.m_connection_errors++;
    m_totals.update_connection_error();
}

void run_stats::update_moved_get_op(struct timeval* ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency)
{
    roll_cur_stats(ts);

    m_cur_stats.m_get_cmd.update_moved_op(bytes_rx, bytes_tx, latency);
    m_cur_stats.m_total_cmd.update_op(bytes_rx, bytes_tx, latency);
    m_totals.update_op(bytes_rx, bytes_tx, latency);
    hdr_record_value_capped(m_get_latency_histogram,latency);
    hdr_record_value_capped(inst_m_get_latency_histogram,latency);
    hdr_record_value_capped(m_totals_latency_histogram,latency);
    hdr_record_value_capped(inst_m_totals_latency_histogram,latency);
}

void run_stats::update_moved_set_op(struct timeval* ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency)
{
    roll_cur_stats(ts);

    m_cur_stats.m_set_cmd.update_moved_op(bytes_rx, bytes_tx, latency);
    m_cur_stats.m_total_cmd.update_moved_op(bytes_rx, bytes_tx, latency);
    m_totals.update_op(bytes_rx, bytes_tx, latency);
    hdr_record_value_capped(m_set_latency_histogram,latency);
    hdr_record_value_capped(inst_m_set_latency_histogram,latency);
    hdr_record_value_capped(m_totals_latency_histogram,latency);
    hdr_record_value_capped(inst_m_totals_latency_histogram,latency);
}

void run_stats::update_moved_arbitrary_op(struct timeval *ts, unsigned int bytes_rx, unsigned int bytes_tx,
                                    unsigned int latency, size_t request_index) {
    roll_cur_stats(ts);

    m_cur_stats.m_ar_commands.at(request_index).update_moved_op(bytes_rx, bytes_tx, latency);
    m_cur_stats.m_total_cmd.update_op(bytes_rx, bytes_tx, latency);
    m_totals.update_op(bytes_rx, bytes_tx, latency);

    struct hdr_histogram* hist = m_ar_commands_latency_histograms.at(request_index);
    hdr_record_value_capped(hist,latency);
    hdr_record_value_capped(m_totals_latency_histogram,latency);
    hdr_record_value_capped(inst_m_totals_latency_histogram,latency);
}

void run_stats::update_ask_get_op(struct timeval* ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency)
{
    roll_cur_stats(ts);

    m_cur_stats.m_get_cmd.update_ask_op(bytes_rx, bytes_tx, latency);
    m_cur_stats.m_total_cmd.update_ask_op(bytes_rx, bytes_tx, latency);
    m_totals.update_op(bytes_rx, bytes_tx, latency);
    hdr_record_value_capped(m_get_latency_histogram,latency);
    hdr_record_value_capped(inst_m_get_latency_histogram,latency);
    hdr_record_value_capped(m_totals_latency_histogram,latency);
    hdr_record_value_capped(inst_m_totals_latency_histogram,latency);
}

void run_stats::update_ask_set_op(struct timeval* ts, unsigned int bytes_rx, unsigned int bytes_tx, unsigned int latency)
{
    roll_cur_stats(ts);

    m_cur_stats.m_set_cmd.update_ask_op(bytes_rx, bytes_tx, latency);
    m_cur_stats.m_total_cmd.update_ask_op(bytes_rx, bytes_tx, latency);
    m_totals.update_op(bytes_rx, bytes_tx, latency);
    hdr_record_value_capped(m_set_latency_histogram,latency);
    hdr_record_value_capped(inst_m_set_latency_histogram,latency);
    hdr_record_value_capped(m_totals_latency_histogram,latency);
    hdr_record_value_capped(inst_m_totals_latency_histogram,latency);
}

void run_stats::update_ask_arbitrary_op(struct timeval *ts, unsigned int bytes_rx, unsigned int bytes_tx,
                                          unsigned int latency, size_t request_index) {
    roll_cur_stats(ts);

    m_cur_stats.m_ar_commands.at(request_index).update_ask_op(bytes_rx, bytes_tx, latency);
    m_cur_stats.m_total_cmd.update_ask_op(bytes_rx, bytes_tx, latency);
    m_totals.update_op(bytes_rx, bytes_tx, latency);

    struct hdr_histogram* hist = m_ar_commands_latency_histograms.at(request_index);
    hdr_record_value_capped(hist,latency);
    hdr_record_value_capped(m_totals_latency_histogram,latency);
    hdr_record_value_capped(inst_m_totals_latency_histogram,latency);
}

void run_stats::update_wait_op(struct timeval *ts, unsigned int latency)
{
    roll_cur_stats(ts);

    m_cur_stats.m_wait_cmd.update_op(0,0, latency);
    m_cur_stats.m_total_cmd.update_op(0,0, latency);
    m_totals.update_op(0,0, latency);
    hdr_record_value_capped(m_wait_latency_histogram,latency);
    hdr_record_value_capped(inst_m_wait_latency_histogram,latency);
    hdr_record_value_capped(m_totals_latency_histogram,latency);
    hdr_record_value_capped(inst_m_totals_latency_histogram,latency);
}

void run_stats::update_arbitrary_op(struct timeval *ts, unsigned int bytes_rx, unsigned int bytes_tx,
                                    unsigned int latency, size_t request_index) {
    roll_cur_stats(ts);

    m_cur_stats.m_ar_commands.at(request_index).update_op(bytes_rx, bytes_tx, latency);
    m_cur_stats.m_total_cmd.update_op(bytes_rx, bytes_tx, latency);
    m_totals.update_op(bytes_rx, bytes_tx, latency);

    struct hdr_histogram* hist = m_ar_commands_latency_histograms.at(request_index);
    struct hdr_histogram* inst_hist = inst_m_ar_commands_latency_histograms.at(request_index);
    hdr_record_value_capped(hist,latency);
    hdr_record_value_capped(inst_hist,latency);
    hdr_record_value_capped(m_totals_latency_histogram,latency);
    hdr_record_value_capped(inst_m_totals_latency_histogram,latency);
}

unsigned int run_stats::get_duration(void)
{
    return m_cur_stats.m_second;
}

unsigned long int run_stats::get_duration_usec(void)
{
    if (!m_start_time.tv_sec)
        return 0;
    if (m_end_time.tv_sec > 0) {
        return ts_diff(m_start_time, m_end_time);
    } else {
        return ts_diff_now(m_start_time);
    }
}

unsigned long int run_stats::get_total_bytes(void)
{
    return m_totals.m_bytes_rx+m_totals.m_bytes_tx;
}

unsigned long int run_stats::get_total_ops(void)
{
    return m_totals.m_ops;
}

unsigned long int run_stats::get_total_latency(void)
{
    return m_totals.m_latency;
}

unsigned long int run_stats::get_total_connection_errors(void)
{
    return m_totals.m_connection_errors;
}

#define AVERAGE(total, count) \
    ((unsigned int) ((count) > 0 ? (total) / (count) : 0))
#define USEC_FORMAT(value) \
    (value) / 1000000, (value) % 1000000

void run_stats::save_csv_one_sec(FILE *f,
                                 unsigned long int& total_get_ops,
                                 unsigned long int& total_set_ops,
                                 unsigned long int& total_wait_ops) {
    fprintf(f, "Per-Second Benchmark Data\n");
    fprintf(f, "Second,SET Requests,SET Average Latency,SET Total Bytes,SET Total Bytes TX,SET Total Bytes RX,"
               "GET Requests,GET Average Latency,GET Total Bytes,GET Total Bytes TX,GET Total Bytes RX,GET Misses,GET Hits,"
               "WAIT Requests,WAIT Average Latency\n");

    total_get_ops = 0;
    total_set_ops = 0;
    total_wait_ops = 0;
    for (std::list<one_second_stats>::iterator i = m_stats.begin();
         i != m_stats.end(); i++) {

        fprintf(f, "%u,%lu,%u.%06u,%lu,%lu,%lu,%lu,%u.%06u,%lu,%lu,%lu,%u,%u,%lu,%u.%06u\n",
                i->m_second,
                i->m_set_cmd.m_ops,
                USEC_FORMAT(AVERAGE(i->m_set_cmd.m_total_latency, i->m_set_cmd.m_ops)),
                i->m_set_cmd.m_bytes_rx + i->m_set_cmd.m_bytes_tx,
                i->m_set_cmd.m_bytes_tx,
                i->m_set_cmd.m_bytes_rx,
                i->m_get_cmd.m_ops,
                USEC_FORMAT(AVERAGE(i->m_get_cmd.m_total_latency, i->m_get_cmd.m_ops)),
                i->m_get_cmd.m_bytes_rx + i->m_get_cmd.m_bytes_tx,
                i->m_get_cmd.m_bytes_tx,
                i->m_get_cmd.m_bytes_rx,
                i->m_get_cmd.m_misses,
                i->m_get_cmd.m_hits,
                i->m_wait_cmd.m_ops,
                USEC_FORMAT(AVERAGE(i->m_wait_cmd.m_total_latency, i->m_wait_cmd.m_ops)));

        total_set_ops += i->m_set_cmd.m_ops;
        total_get_ops += i->m_get_cmd.m_ops;
        total_wait_ops += i->m_wait_cmd.m_ops;
    }
}


std::vector<one_sec_cmd_stats> run_stats::get_one_sec_cmd_stats_get() {
    std::vector<one_sec_cmd_stats> result;
    result.reserve(m_stats.size());
    for (std::list<one_second_stats>::iterator i = m_stats.begin();
         i != m_stats.end(); i++) {
            result.push_back(i->m_get_cmd);    
    }
    return result;
}

std::vector<one_sec_cmd_stats> run_stats::get_one_sec_cmd_stats_set() {
    std::vector<one_sec_cmd_stats> result;
    result.reserve(m_stats.size());
    for (std::list<one_second_stats>::iterator i = m_stats.begin();
         i != m_stats.end(); i++) {
            result.push_back(i->m_set_cmd);    
    }
    return result;
}

std::vector<one_sec_cmd_stats> run_stats::get_one_sec_cmd_stats_wait() {
    std::vector<one_sec_cmd_stats> result;
    result.reserve(m_stats.size());
    for (std::list<one_second_stats>::iterator i = m_stats.begin();
         i != m_stats.end(); i++) {
            result.push_back(i->m_wait_cmd);
    }
    return result;
}

std::vector<one_sec_cmd_stats> run_stats::get_one_sec_cmd_stats_totals() {
    std::vector<one_sec_cmd_stats> result;
    result.reserve(m_stats.size());
    for (std::list<one_second_stats>::iterator i = m_stats.begin();
         i != m_stats.end(); i++) {
            result.push_back(i->m_total_cmd);
    }
    return result;
}


std::vector<one_sec_cmd_stats> run_stats::get_one_sec_cmd_stats_arbitrary_command( unsigned int pos ){
    std::vector<one_sec_cmd_stats> result;
    result.reserve(m_stats.size());
    for (std::list<one_second_stats>::iterator i = m_stats.begin();
         i != m_stats.end(); i++) {
            result.push_back(i->m_ar_commands.at(pos));
    }
    return result;
}

std::vector<unsigned int> run_stats::get_one_sec_cmd_stats_timestamp() {
    std::vector<unsigned int> result;
    result.reserve(m_stats.size());
    for (std::list<one_second_stats>::iterator i = m_stats.begin();
         i != m_stats.end(); i++) {
            result.push_back(i->m_second);    
    }
    return result;
}


void run_stats::save_csv_one_sec_cluster(FILE *f) {
    fprintf(f, "\nPer-Second Benchmark Cluster Data\n");
    fprintf(f, "Second,SET Moved,SET Ask,GET Moved,GET Ask\n");

    for (std::list<one_second_stats>::iterator i = m_stats.begin();
         i != m_stats.end(); i++) {

        fprintf(f, "%u,%u,%u,%u,%u\n",
                i->m_second,
                i->m_set_cmd.m_moved,
                i->m_set_cmd.m_ask,
                i->m_get_cmd.m_moved,
                i->m_get_cmd.m_ask);
    }
}

void run_stats::save_csv_set_get_commands(FILE *f, bool cluster_mode) {
    unsigned long int total_get_ops;
    unsigned long int total_set_ops;
    unsigned long int total_wait_ops;

    // save per second data
    save_csv_one_sec(f, total_get_ops, total_set_ops, total_wait_ops);

    // save latency data
    fprintf(f, "\n" "Full-Test GET Latency\n");
    fprintf(f, "Latency (<= msec),Percent\n");
    struct hdr_iter iter;
    struct hdr_iter_percentiles * percentiles;
    
    hdr_iter_percentile_init(&iter, m_get_latency_histogram, LATENCY_HDR_GRANULARITY);
    percentiles = &iter.specifics.percentiles;
    while (hdr_iter_next(&iter)){
        double  value = iter.highest_equivalent_value / (double) LATENCY_HDR_RESULTS_MULTIPLIER;
        fprintf(f, "%8.3f,%.3f\n", value,percentiles->percentile);
    }
    fprintf(f, "\n" "Full-Test SET Latency\n");
    fprintf(f, "Latency (<= msec),Percent\n");
    hdr_iter_percentile_init(&iter, m_set_latency_histogram, LATENCY_HDR_GRANULARITY);
    percentiles = &iter.specifics.percentiles;
    while (hdr_iter_next(&iter)){
        double value = iter.highest_equivalent_value / (double) LATENCY_HDR_RESULTS_MULTIPLIER;
        fprintf(f, "%8.3f,%.3f\n", value,percentiles->percentile);
    }

    fprintf(f, "\n" "Full-Test WAIT Latency\n");
    fprintf(f, "Latency (<= msec),Percent\n");
    hdr_iter_percentile_init(&iter, m_wait_latency_histogram, LATENCY_HDR_GRANULARITY);
    percentiles = &iter.specifics.percentiles;
    while (hdr_iter_next(&iter)){
        double value = iter.highest_equivalent_value / (double) LATENCY_HDR_RESULTS_MULTIPLIER;
        fprintf(f, "%8.3f,%.3f\n", value,percentiles->percentile);
    }

    // cluster mode data
    if (cluster_mode) {
        save_csv_one_sec_cluster(f);
    }
}

void run_stats::save_csv_arbitrary_commands_one_sec(FILE *f,
                                                    arbitrary_command_list& command_list,
                                                    std::vector<unsigned long int>& total_arbitrary_commands_ops) {
    fprintf(f, "Per-Second Benchmark Arbitrary Commands Data\n");

    // print header
    fprintf(f, "Second");
    for (unsigned int i=0; i<command_list.size(); i++) {
        std::string command_name = command_list[i].command_name;

        fprintf(f, ",%s Requests,%s Average Latency,%s Total Bytes,%s Total Bytes TX,%s Total Bytes RX",
                command_name.c_str(),
                command_name.c_str(),
                command_name.c_str(),
                command_name.c_str(),
                command_name.c_str());
    }
    fprintf(f, "\n");

    // print data
    for (std::list<one_second_stats>::iterator stat = m_stats.begin();
         stat != m_stats.end(); stat++) {

        fprintf(f, "%u,", stat->m_second);

        for (unsigned int i=0; i<stat->m_ar_commands.size(); i++) {
            one_sec_cmd_stats& arbitrary_command_stats = stat->m_ar_commands[i];

            fprintf(f, "%lu,%u.%06u,%lu,%lu,%lu,",
                arbitrary_command_stats.m_ops,
                USEC_FORMAT(AVERAGE(arbitrary_command_stats.m_total_latency, arbitrary_command_stats.m_ops)),
                arbitrary_command_stats.m_bytes_rx+arbitrary_command_stats.m_bytes_tx,
                arbitrary_command_stats.m_bytes_tx,
                arbitrary_command_stats.m_bytes_rx
                );

            total_arbitrary_commands_ops.at(i) += arbitrary_command_stats.m_ops;
        }

        fprintf(f, "\n");
    }
}

void run_stats::save_csv_arbitrary_commands(FILE *f, arbitrary_command_list& command_list) {
    std::vector<unsigned long int> total_arbitrary_commands_ops(command_list.size());

    // save per second data
    save_csv_arbitrary_commands_one_sec(f, command_list, total_arbitrary_commands_ops);

    // save latency data
    for (unsigned int i=0; i<command_list.size(); i++) {
        std::string command_name = command_list[i].command_name;

        fprintf(f, "\n" "Full-Test %s Latency\n", command_name.c_str());
        fprintf(f, "Latency (<= msec),Percent\n");

        struct hdr_iter iter;
        struct hdr_iter_percentiles * percentiles;
        
        struct hdr_histogram* hist = m_ar_commands_latency_histograms.at(i);
        hdr_iter_percentile_init(&iter, hist, LATENCY_HDR_GRANULARITY);
        percentiles = &iter.specifics.percentiles;
        while (hdr_iter_next(&iter)){
            double value = iter.highest_equivalent_value / (double) LATENCY_HDR_RESULTS_MULTIPLIER;
            fprintf(f, "%8.3f,%.3f\n", value,percentiles->percentile);
        }
    }
}

bool run_stats::save_hdr_percentiles_print_format(struct hdr_histogram* hdr, char* filename){
    bool result = false;
    if(hdr_total_count( hdr )>0){
        // Prepare output file
        FILE *hdr_outfile;

        hdr_outfile = fopen(filename, "w");
        if (!hdr_outfile){
            perror(filename);
            return result;
        }
        hdr_percentiles_print(
                hdr,
                hdr_outfile,                      // File to write to
                LATENCY_HDR_GRANULARITY,          // Granularity of printed values
                LATENCY_HDR_RESULTS_MULTIPLIER,   // Multiplier for results
                CLASSIC);                         // Format CLASSIC/CSV supported.
        fclose(hdr_outfile);
        result=true;
    }
    return result;
}

bool run_stats::save_hdr_log_format(struct hdr_histogram* hdr, char* filename, char* header){
    bool result = false;
    if(hdr_total_count( hdr )>0){
        // Prepare output file
        FILE *hdr_outfile;
        struct timespec start_timespec;
        struct timespec end_timespec;
        TIMEVAL_TO_TIMESPEC(&m_start_time, &start_timespec);
        TIMEVAL_TO_TIMESPEC(&m_end_time, &end_timespec);
        hdr_outfile = fopen(filename, "w");
        if (!hdr_outfile){
            perror(filename);
            return result;
        }
        struct hdr_log_writer w;
        hdr_log_writer_init(&w);
        hdr_log_write_header(&w, hdr_outfile, header, &start_timespec);
        hdr_log_write(&w, hdr_outfile, &start_timespec, &end_timespec, hdr);
        fclose(hdr_outfile);
        result = true;
    }
    return result;
}

bool run_stats::save_hdr_full_run(benchmark_config *config,int run_number){
    if (strcmp(config->hdr_prefix,"") && (hdr_total_count( m_totals.latency_histogram )>0) ){
        // Prepare output file
        char fmtbuf[1024];

        snprintf(fmtbuf, sizeof(fmtbuf) - 1, "%s_FULL_RUN_%d.txt", config->hdr_prefix, run_number);
        fprintf(stderr, "Writing Full Run command HDR latency histogram results to %s...\n", fmtbuf);
        save_hdr_percentiles_print_format(m_totals.latency_histogram,fmtbuf);

        snprintf(fmtbuf, sizeof(fmtbuf) - 1, "%s_FULL_RUN_%d.hgrm", config->hdr_prefix, run_number);
        fprintf(stderr, "Writing Full Run command HDR latency histogram results in HistogramLogProcessor format to %s...\n", fmtbuf);
        save_hdr_log_format(m_totals.latency_histogram,fmtbuf,(char*)"Full Run command HDR latency histogram results");
    }
    return true;
}

bool run_stats::save_hdr_set_command(benchmark_config *config,int run_number) {
    if (strcmp(config->hdr_prefix,"") && (hdr_total_count( m_set_latency_histogram )>0) ){
        // Prepare output file
        char fmtbuf[1024];

        snprintf(fmtbuf, sizeof(fmtbuf) - 1, "%s_SET_command_run_%d.txt", config->hdr_prefix, run_number);
        fprintf(stderr, "Writing SET command HDR latency histogram results to %s...\n", fmtbuf);
        save_hdr_percentiles_print_format(m_set_latency_histogram,fmtbuf);

        snprintf(fmtbuf, sizeof(fmtbuf) - 1, "%s_SET_command_run_%d.hgrm", config->hdr_prefix, run_number);
        fprintf(stderr, "Writing SET command HDR latency histogram results in HistogramLogProcessor format to %s...\n", fmtbuf);
        save_hdr_log_format(m_set_latency_histogram,fmtbuf,(char*)"SET command HDR latency histogram results");
    }
    return true;
}

bool run_stats::save_hdr_get_command(benchmark_config *config, int run_number){
    if (strcmp(config->hdr_prefix,"") && (hdr_total_count( m_get_latency_histogram )>0) ){
        // Prepare output file
        char fmtbuf[1024];

        snprintf(fmtbuf, sizeof(fmtbuf) - 1, "%s_GET_command_run_%d.txt", config->hdr_prefix, run_number);
        fprintf(stderr, "Writing GET command HDR latency histogram results to %s...\n", fmtbuf);
        save_hdr_percentiles_print_format(m_get_latency_histogram,fmtbuf);

        snprintf(fmtbuf, sizeof(fmtbuf) - 1, "%s_GET_command_run_%d.hgrm", config->hdr_prefix, run_number);
        fprintf(stderr, "Writing GET command HDR latency histogram results in HistogramLogProcessor format to %s...\n", fmtbuf);
        save_hdr_log_format(m_get_latency_histogram,fmtbuf,(char*)"GET command HDR latency histogram results");
    }
    return true;
}

bool run_stats::save_hdr_arbitrary_commands(benchmark_config *config,int run_number) {
    // save latency datacommand_list
    if (strcmp(config->hdr_prefix,"")) {

        arbitrary_command_list& command_list = *config->arbitrary_commands;
        for (unsigned int i=0; i<command_list.size(); i++) {
            std::string command_name = command_list[i].command_name;

            // Prepare output file
            char fmtbuf[1024];
            struct hdr_histogram* hist = m_ar_commands_latency_histograms.at(i);

            snprintf(fmtbuf, sizeof(fmtbuf) - 1, "%s_%s_command_run_%d.txt", config->hdr_prefix, command_name.c_str(),run_number);
            fprintf(stderr, "Writing %s command HDR latency histogram results to %s...\n", command_name.c_str(), fmtbuf);
            save_hdr_percentiles_print_format(hist,fmtbuf);

            snprintf(fmtbuf, sizeof(fmtbuf) - 1, "%s_%s_command_run_%d.hgrm", config->hdr_prefix, command_name.c_str(), run_number);
            fprintf(stderr, "Writing %s command HDR latency histogram results in HistogramLogProcessor format to %s...\n", command_name.c_str(), fmtbuf);
            char header[1024];
            snprintf(header, sizeof(header) - 1, "%s command HDR latency histogram results", command_name.c_str());
            save_hdr_log_format(hist,fmtbuf,header);
        }
    }
    return true;
}

bool run_stats::save_csv(const char *filename, benchmark_config *config)
{
    FILE *f = fopen(filename, "w");
    if (!f) {
        perror(filename);
        return false;
    }

    if (print_arbitrary_commands_results()) {
        save_csv_arbitrary_commands(f, *config->arbitrary_commands);
    } else {
        save_csv_set_get_commands(f, config->cluster_mode);
    }

    fclose(f);
    return true;
}

void run_stats::debug_dump(void)
{
    benchmark_debug_log("run_stats: start_time={%u,%u} end_time={%u,%u}\n",
                        m_start_time.tv_sec, m_start_time.tv_usec,
                        m_end_time.tv_sec, m_end_time.tv_usec);

    for (std::list<one_second_stats>::iterator i = m_stats.begin();
         i != m_stats.end(); i++) {

        benchmark_debug_log("  %u: get latency=%u.%ums, set latency=%u.%ums, wait latency=%u.%ums"
                            "m_ops_set/get/wait=%u/%u/%u, m_bytes_set/get=%u/%u, m_get_hit/miss=%u/%u\n",
                            i->m_second,
                            USEC_FORMAT(AVERAGE(i->m_get_cmd.m_total_latency, i->m_get_cmd.m_ops)),
                            USEC_FORMAT(AVERAGE(i->m_set_cmd.m_total_latency, i->m_set_cmd.m_ops)),
                            USEC_FORMAT(AVERAGE(i->m_wait_cmd.m_total_latency, i->m_wait_cmd.m_ops)),
                            i->m_set_cmd.m_ops,
                            i->m_get_cmd.m_ops,
                            i->m_wait_cmd.m_ops,
                            i->m_set_cmd.m_bytes_rx+i->m_set_cmd.m_bytes_tx,
                            i->m_get_cmd.m_bytes_rx+i->m_get_cmd.m_bytes_tx,
                            i->m_get_cmd.m_hits,
                            i->m_get_cmd.m_misses);
    }
}

bool one_second_stats_predicate(const one_second_stats& a, const one_second_stats& b)
{
    return a.m_second < b.m_second;
}

void run_stats::aggregate_average(const std::vector<run_stats>& all_stats)
{
    for (std::vector<run_stats>::const_iterator i = all_stats.begin();
         i != all_stats.end(); i++) {

        totals i_totals;
        i_totals.setup_arbitrary_commands(m_totals.m_ar_commands.size());

        i->summarize(i_totals);
        m_totals.add(i_totals);

        // aggregate latency data
        hdr_add(m_get_latency_histogram,i->m_get_latency_histogram);
        hdr_add(m_set_latency_histogram,i->m_set_latency_histogram);
        hdr_add(m_wait_latency_histogram,i->m_wait_latency_histogram);
        hdr_add(m_totals_latency_histogram,i->m_totals_latency_histogram);

        for (unsigned int j=0; j < i->m_ar_commands_latency_histograms.size(); j++) {
            hdr_add(m_ar_commands_latency_histograms.at(j),i->m_ar_commands_latency_histograms.at(j));
        }
    }

    m_totals.m_set_cmd.aggregate_average(all_stats.size());
    m_totals.m_get_cmd.aggregate_average(all_stats.size());
    m_totals.m_wait_cmd.aggregate_average(all_stats.size());
    m_totals.m_total_cmd.aggregate_average(all_stats.size());
    m_totals.m_ar_commands.aggregate_average(all_stats.size());
    m_totals.m_ops_sec /= all_stats.size();
    m_totals.m_hits_sec /= all_stats.size();
    m_totals.m_misses_sec /= all_stats.size();
    m_totals.m_moved_sec /= all_stats.size();
    m_totals.m_ask_sec /= all_stats.size();
    m_totals.m_bytes_sec /= all_stats.size();
    m_totals.m_latency /= all_stats.size();
}

void run_stats::merge(const run_stats& other, int iteration)
{
    bool new_stats = false;

    m_start_time = timeval_factorial_average( m_start_time, other.m_start_time, iteration );
    m_end_time =   timeval_factorial_average( m_end_time,   other.m_end_time,   iteration );

    // If any run was interrupted, mark the merged result as interrupted
    if (other.m_interrupted) {
        m_interrupted = true;
    }

    // aggregate the one_second_stats vectors. this is not efficient
    // but it's not really important (small numbers, not realtime)
    for (std::list<one_second_stats>::const_iterator other_i = other.m_stats.begin();
         other_i != other.m_stats.end(); other_i++) {

        // find ours
        bool merged = false;
        for (std::list<one_second_stats>::iterator i = m_stats.begin();
             i != m_stats.end(); i++) {
            if (i->m_second == other_i->m_second) {
                i->merge(*other_i);
                merged = true;
                break;
            }
        }

        if (!merged) {
            m_stats.push_back(*other_i);
            new_stats = true;
        }
    }

    if (new_stats) {
        m_stats.sort(one_second_stats_predicate);
    }

    // aggregate totals
    m_totals.add(other.m_totals);

    // aggregate latency data
    hdr_add(m_totals_latency_histogram,other.m_totals.latency_histogram);
    hdr_add(m_get_latency_histogram,other.m_get_latency_histogram);
    hdr_add(m_set_latency_histogram,other.m_set_latency_histogram);
    hdr_add(m_wait_latency_histogram,other.m_wait_latency_histogram);

    for (unsigned int j=0; j < other.m_ar_commands_latency_histograms.size(); j++) {
        hdr_add(m_ar_commands_latency_histograms.at(j),other.m_ar_commands_latency_histograms.at(j));
    }
}

void run_stats::summarize(totals& result) const
{
    // aggregate all one_second_stats
    one_second_stats totals(0);
    totals.setup_arbitrary_commands(m_cur_stats.m_ar_commands.size());

    for (std::list<one_second_stats>::const_iterator i = m_stats.begin();
         i != m_stats.end(); i++) {
        totals.merge(*i);
    }

    // Also include current stats that haven't been rolled yet
    totals.merge(m_cur_stats);

    unsigned long int test_duration_usec = ts_diff(m_start_time, m_end_time);

    // total ops, bytes
    result.m_ops = totals.m_set_cmd.m_ops + totals.m_get_cmd.m_ops + totals.m_wait_cmd.m_ops + totals.m_ar_commands.ops();
    result.m_bytes_rx = totals.m_set_cmd.m_bytes_rx + totals.m_get_cmd.m_bytes_rx + totals.m_ar_commands.bytes();
    result.m_bytes_tx = totals.m_set_cmd.m_bytes_tx + totals.m_get_cmd.m_bytes_tx + totals.m_ar_commands.bytes();

    // cmd/sec
    result.m_set_cmd.summarize(totals.m_set_cmd, test_duration_usec);
    result.m_get_cmd.summarize(totals.m_get_cmd, test_duration_usec);
    result.m_wait_cmd.summarize(totals.m_wait_cmd, test_duration_usec);
    result.m_ar_commands.summarize(totals.m_ar_commands, test_duration_usec);

    // hits,misses / sec
    result.m_hits_sec = (double) totals.m_get_cmd.m_hits / test_duration_usec * 1000000;
    result.m_misses_sec = (double) totals.m_get_cmd.m_misses / test_duration_usec * 1000000;

    // total/sec
    result.m_ops_sec = (double) result.m_ops / test_duration_usec * 1000000;
    if (result.m_ops > 0) {
        result.m_latency = (double) ((totals.m_set_cmd.m_total_latency +
                                      totals.m_get_cmd.m_total_latency +
                                      totals.m_wait_cmd.m_total_latency +
                                      totals.m_ar_commands.total_latency()) /
                                     result.m_ops) /
                                    1000;
    } else {
        result.m_latency = 0;
    }

    result.m_bytes_sec = ((result.m_bytes_rx + result.m_bytes_tx) / 1024.0) / test_duration_usec * 1000000;
    result.m_bytes_sec_rx = (result.m_bytes_rx / 1024.0) / test_duration_usec * 1000000;
    result.m_bytes_sec_tx = (result.m_bytes_tx / 1024.0) / test_duration_usec * 1000000;
    result.m_moved_sec = (double) (totals.m_set_cmd.m_moved + totals.m_get_cmd.m_moved) / test_duration_usec * 1000000;
    result.m_ask_sec = (double) (totals.m_set_cmd.m_ask + totals.m_get_cmd.m_ask) / test_duration_usec * 1000000;

    // connection errors/sec
    result.m_connection_errors = totals.m_connection_errors;
    result.m_connection_errors_sec = (double) totals.m_connection_errors / test_duration_usec * 1000000;
}

void result_print_to_json(json_handler * jsonhandler, const char * type, double ops_sec,
                          double hits, double miss, double moved, double ask, double kbs, double kbs_rx, double kbs_tx,
                          double latency, long m_total_latency, long ops, double connection_errors_sec, long connection_errors,
                          std::vector<double> quantile_list, struct hdr_histogram* latency_histogram,
                          std::vector<unsigned int> timestamps,
                          std::vector<one_sec_cmd_stats> timeserie_stats )
{
    if (jsonhandler != NULL){ // Added for double verification in case someone accidently send NULL.
        jsonhandler->open_nesting(type);
        jsonhandler->write_obj("Count","%lld", ops);
        jsonhandler->write_obj("Ops/sec","%.2f", ops_sec);
        jsonhandler->write_obj("Hits/sec","%.2f", hits);
        jsonhandler->write_obj("Misses/sec","%.2f", miss);

        if (moved >= 0)
            jsonhandler->write_obj("MOVED/sec","%.2f", moved);

        if (ask >= 0)
            jsonhandler->write_obj("ASK/sec","%.2f", ask);

        jsonhandler->write_obj("Connection Errors/sec","%.2f", connection_errors_sec);
        jsonhandler->write_obj("Connection Errors","%lld", connection_errors);

        const bool has_samples = hdr_total_count(latency_histogram)>0;
        const double avg_latency = latency;
        const double min_latency = has_samples ? (hdr_min(latency_histogram) * 1.0)/ (double) LATENCY_HDR_RESULTS_MULTIPLIER : 0.0;
        const double max_latency = has_samples ? (hdr_max(latency_histogram) * 1.0)/ (double) LATENCY_HDR_RESULTS_MULTIPLIER : 0.0;
        // to be retrocompatible
        jsonhandler->write_obj("Latency","%.3f", avg_latency);
        jsonhandler->write_obj("Average Latency","%.3f", avg_latency);
        jsonhandler->write_obj("Accumulated Latency","%lld", m_total_latency / LATENCY_HDR_RESULTS_MULTIPLIER);
        jsonhandler->write_obj("Min Latency","%.3f", min_latency);
        jsonhandler->write_obj("Max Latency","%.3f", max_latency);
        // preserve KB/sec for backwards compatibility
        jsonhandler->write_obj("KB/sec","%.2f", kbs);
        jsonhandler->write_obj("KB/sec RX/TX","%.2f", kbs);
        jsonhandler->write_obj("KB/sec RX","%.2f", kbs_rx);
        jsonhandler->write_obj("KB/sec TX","%.2f", kbs_tx);
        jsonhandler->open_nesting("Time-Serie");
        for (std::size_t i = 0; i < timeserie_stats.size(); i++){
            char timestamp_str[16];
            one_sec_cmd_stats cmd_stats = timeserie_stats[i];
            const unsigned int timestamp = timestamps[i];
            const bool sec_has_samples = cmd_stats.m_ops > 0;
            snprintf(timestamp_str,sizeof(timestamp_str)-1,"%d", timestamp);
            jsonhandler->open_nesting(timestamp_str);
            jsonhandler->write_obj("Bytes RX","%lld", cmd_stats.m_bytes_rx);
            jsonhandler->write_obj("Bytes TX","%lld", cmd_stats.m_bytes_tx);
            jsonhandler->write_obj("Count","%lld", cmd_stats.m_ops);
            if (sec_has_samples){
                jsonhandler->write_obj("Average Latency","%.3f", cmd_stats.m_avg_latency);
                jsonhandler->write_obj("Accumulated Latency","%lld", cmd_stats.m_total_latency / LATENCY_HDR_RESULTS_MULTIPLIER);
                jsonhandler->write_obj("Min Latency","%.3f", cmd_stats.m_min_latency);
                jsonhandler->write_obj("Max Latency","%.3f", cmd_stats.m_max_latency);
                for (std::size_t i = 0; i < quantile_list.size(); i++){
                    if (i < cmd_stats.summarized_quantile_values.size()){
                        const float quantile = quantile_list[i];
                        char quantile_header[8];
                        snprintf(quantile_header,sizeof(quantile_header)-1,"p%.2f", quantile);
                        const double value = cmd_stats.summarized_quantile_values[i];
                        jsonhandler->write_obj((char *)quantile_header,"%.3f", value);
                    }
                }
            }
            jsonhandler->close_nesting();
        }
        jsonhandler->close_nesting();
        jsonhandler->open_nesting("Percentile Latencies");
        for (std::size_t i = 0; i < quantile_list.size(); i++){
            const float quantile = quantile_list[i];
            char quantile_header[8];
            snprintf(quantile_header,sizeof(quantile_header)-1,"p%.3f", quantile);
            const double value = hdr_value_at_percentile(latency_histogram, quantile )/ (double) LATENCY_HDR_RESULTS_MULTIPLIER;
            jsonhandler->write_obj((char *)quantile_header,"%.3f", value);
        }
        jsonhandler->open_nesting("Histogram log format");
        char* encoded_histogram;
        hdr_string_write(&encoded_histogram,latency_histogram);
        jsonhandler->write_obj("Compressed Histogram","\"%s\"", encoded_histogram);
        free(encoded_histogram);
        jsonhandler->close_nesting();
        jsonhandler->close_nesting();
        jsonhandler->close_nesting();
    }
}

void histogram_print(FILE * out, json_handler * jsonhandler, const char * type, float msec, float percent)
{
    fprintf(out, "%-6s %8.3f %12.3f\n", type, msec, percent);
    if (jsonhandler != NULL){
        jsonhandler->open_nesting(NULL);
        jsonhandler->write_obj("<=msec","%.3f", msec);
        jsonhandler->write_obj("percent","%.3f", percent);
        jsonhandler->close_nesting();
    }
}

bool run_stats::print_arbitrary_commands_results() {
    return m_totals.m_ar_commands.size() > 0;
}

void run_stats::print_type_column(output_table &table, arbitrary_command_list& command_list) {
    table_el el;
    table_column column;

    // Type column
    column.column_size = MAX(6, command_list.get_max_command_name_length()) + 1;
    assert(column.column_size < 100 && "command name too long");

    // set enough space according to size of command name
    char buf[200];
    snprintf(buf, sizeof(buf), "%%-%us ", column.column_size);
    std::string type_col_format(buf);
    memset(buf, '-', column.column_size + 1);
    buf[column.column_size + 1] = '\0';

    column.elements.push_back(*el.init_str(type_col_format, "Type"));
    column.elements.push_back(*el.init_str("%s", buf));

    if (print_arbitrary_commands_results()) {
        for (unsigned int i=0; i<command_list.size(); i++) {
            // format command name
            std::string command_name = command_list[i].command_name;

            std::transform(command_name.begin(), command_name.end(), command_name.begin(), ::tolower);
            command_name[0] = static_cast<char>(toupper(command_name[0]));
            command_name.append("s");

            column.elements.push_back(*el.init_str(type_col_format, command_name));
        }
    } else {
        column.elements.push_back(*el.init_str(type_col_format, "Sets"));
        column.elements.push_back(*el.init_str(type_col_format, "Gets"));
        column.elements.push_back(*el.init_str(type_col_format, "Waits"));
    }
    column.elements.push_back(*el.init_str(type_col_format, "Totals"));

    table.add_column(column);
}

void run_stats::print_ops_sec_column(output_table &table) {
    table_el el;
    table_column column(12);

    column.elements.push_back(*el.init_str("%12s ", "Ops/sec"));
    column.elements.push_back(*el.init_str("%s", "-------------"));

    if (print_arbitrary_commands_results()) {
        for (unsigned int i=0; i<m_totals.m_ar_commands.size(); i++) {
            column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_ar_commands[i].m_ops_sec));
        }
    } else {
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_set_cmd.m_ops_sec));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_get_cmd.m_ops_sec));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_wait_cmd.m_ops_sec));
    }
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_ops_sec));

    table.add_column(column);
}
void run_stats::print_hits_sec_column(output_table &table) {
    table_el el;
    table_column column(12);

    column.elements.push_back(*el.init_str("%12s ", "Hits/sec"));
    column.elements.push_back(*el.init_str("%s", "-------------"));
    column.elements.push_back(*el.init_str("%12s ", "---"));
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_hits_sec));
    column.elements.push_back(*el.init_str("%12s ", "---"));
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_hits_sec));

    table.add_column(column);
}

void run_stats::print_missess_sec_column(output_table &table) {
    table_el el;
    table_column column(12);

    column.elements.push_back(*el.init_str("%12s ", "Misses/sec"));
    column.elements.push_back(*el.init_str("%s", "-------------"));
    column.elements.push_back(*el.init_str("%12s ", "---"));
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_misses_sec));
    column.elements.push_back(*el.init_str("%12s ", "---"));
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_misses_sec));

    table.add_column(column);
}

void run_stats::print_moved_sec_column(output_table &table) {
    table_el el;
    table_column column(12);

    column.elements.push_back(*el.init_str("%12s ", "MOVED/sec"));
    column.elements.push_back(*el.init_str("%s", "-------------"));

    if (print_arbitrary_commands_results()) {
        for (unsigned int i=0; i<m_totals.m_ar_commands.size(); i++) {
            column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_ar_commands[i].m_moved_sec));
        }
    } else {
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_set_cmd.m_moved_sec));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_get_cmd.m_moved_sec));
        column.elements.push_back(*el.init_str("%12s ", "---"));

    }
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_moved_sec));
    table.add_column(column);
}

void run_stats::print_ask_sec_column(output_table &table) {
    table_el el;
    table_column column(12);

    column.elements.push_back(*el.init_str("%12s ", "ASK/sec"));
    column.elements.push_back(*el.init_str("%s", "-------------"));
    if (print_arbitrary_commands_results()) {
        for (unsigned int i=0; i<m_totals.m_ar_commands.size(); i++) {
            column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_ar_commands[i].m_ask_sec));
        }
    } else {
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_set_cmd.m_ask_sec));
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_get_cmd.m_ask_sec));
    column.elements.push_back(*el.init_str("%12s ", "---"));

    }
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_ask_sec));
    table.add_column(column);
}

void run_stats::print_avg_latency_column(output_table &table) {
    table_el el;
    table_column column(15);

    safe_hdr_histogram m_totals_latency_histogram;
    hdr_add(m_totals_latency_histogram,m_set_latency_histogram);
    hdr_add(m_totals_latency_histogram,m_get_latency_histogram);
    hdr_add(m_totals_latency_histogram,m_wait_latency_histogram);

    column.elements.push_back(*el.init_str("%15s ", "Avg. Latency"));
    column.elements.push_back(*el.init_str("%s", "----------------"));

    if (print_arbitrary_commands_results()) {
        for (unsigned int i=0; i<m_totals.m_ar_commands.size(); i++) {
            column.elements.push_back(*el.init_double("%15.05f ", hdr_mean(m_ar_commands_latency_histograms[i])/ (double) LATENCY_HDR_RESULTS_MULTIPLIER));
            hdr_add(m_totals_latency_histogram,m_ar_commands_latency_histograms[i]);
        }
    } else {
        const bool has_set_ops = hdr_total_count(m_set_latency_histogram) > 0;
        const bool has_get_ops = hdr_total_count(m_get_latency_histogram) > 0;
        const bool has_wait_ops = hdr_total_count(m_wait_latency_histogram) > 0;
        if(has_set_ops){
            column.elements.push_back(*el.init_double("%15.05f ", hdr_mean(m_set_latency_histogram)/ (double) LATENCY_HDR_RESULTS_MULTIPLIER));
        } else {
            column.elements.push_back(*el.init_str("%15s ", "---"));
        }
        if(has_get_ops){
            column.elements.push_back(*el.init_double("%15.05f ", hdr_mean(m_get_latency_histogram)/ (double) LATENCY_HDR_RESULTS_MULTIPLIER));
        } else {
            column.elements.push_back(*el.init_str("%15s ", "---"));
        }
        if(has_wait_ops){
            column.elements.push_back(*el.init_double("%15.05f ", hdr_mean(m_wait_latency_histogram)/ (double) LATENCY_HDR_RESULTS_MULTIPLIER));
        } else {
            column.elements.push_back(*el.init_str("%15s ", "---"));
        }
    }
    column.elements.push_back(*el.init_double("%15.05f ", hdr_mean(m_totals_latency_histogram)/ (double) LATENCY_HDR_RESULTS_MULTIPLIER));

    table.add_column(column);
}

void run_stats::print_quantile_latency_column(output_table &table, double quantile, char* label) {
    table_el el;
    table_column column(15);

    safe_hdr_histogram m_totals_latency_histogram;
    hdr_add(m_totals_latency_histogram,m_set_latency_histogram);
    hdr_add(m_totals_latency_histogram,m_get_latency_histogram);
    hdr_add(m_totals_latency_histogram,m_wait_latency_histogram);

    column.elements.push_back(*el.init_str("%15s ", label ));
    column.elements.push_back(*el.init_str("%s", "----------------"));

    if (print_arbitrary_commands_results()) {
        for (unsigned int i=0; i<m_totals.m_ar_commands.size(); i++) {
            column.elements.push_back(*el.init_double("%15.05f ", hdr_value_at_percentile(m_ar_commands_latency_histograms[i], quantile ) / (double) LATENCY_HDR_RESULTS_MULTIPLIER));
            hdr_add(m_totals_latency_histogram,m_ar_commands_latency_histograms[i]);
        }
    } else {
        const bool has_set_ops = hdr_total_count(m_set_latency_histogram) > 0;
        const bool has_get_ops = hdr_total_count(m_get_latency_histogram) > 0;
        const bool has_wait_ops = hdr_total_count(m_wait_latency_histogram) > 0;
        if(has_set_ops){
            column.elements.push_back(*el.init_double("%15.05f ", hdr_value_at_percentile(m_set_latency_histogram, quantile )/ (double) LATENCY_HDR_RESULTS_MULTIPLIER));
        } else {
            column.elements.push_back(*el.init_str("%15s ", "---"));
        }
        if(has_get_ops){
            column.elements.push_back(*el.init_double("%15.05f ", hdr_value_at_percentile(m_get_latency_histogram, quantile )/ (double) LATENCY_HDR_RESULTS_MULTIPLIER));
        } else {
            column.elements.push_back(*el.init_str("%15s ", "---"));
        }
        if(has_wait_ops){
            column.elements.push_back(*el.init_double("%15.05f ", hdr_value_at_percentile(m_wait_latency_histogram, quantile )/ (double) LATENCY_HDR_RESULTS_MULTIPLIER));
        } else {
            column.elements.push_back(*el.init_str("%15s ", "---"));
        }
    }

    column.elements.push_back(*el.init_double("%15.05f ", hdr_value_at_percentile(m_totals_latency_histogram, quantile )/ (double) LATENCY_HDR_RESULTS_MULTIPLIER));

    table.add_column(column);
}

void run_stats::print_kb_sec_column(output_table &table) {
    table_el el;
    table_column column(12);

    column.elements.push_back(*el.init_str("%12s ", "KB/sec"));
    column.elements.push_back(*el.init_str("%s", "-------------"));

    if (print_arbitrary_commands_results()) {
        for (unsigned int i=0; i<m_totals.m_ar_commands.size(); i++) {
            column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_ar_commands[i].m_bytes_sec));
        }
    } else {
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_set_cmd.m_bytes_sec));
        column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_get_cmd.m_bytes_sec));
        column.elements.push_back(*el.init_str("%12s ", "---"));
    }
    column.elements.push_back(*el.init_double("%12.2f ", m_totals.m_bytes_sec));

    table.add_column(column);
}

void run_stats::print_json(json_handler *jsonhandler, arbitrary_command_list& command_list, bool cluster_mode) {
    if (jsonhandler != NULL){ // Added for double verification in case someone accidently send NULL.
        jsonhandler->open_nesting("Runtime");
        const unsigned long long start_time_ms = (m_start_time.tv_sec * 1000000 + m_start_time.tv_usec)/1000;
        const unsigned long long end_time_ms = (m_end_time.tv_sec * 1000000 + m_end_time.tv_usec)/1000;
        jsonhandler->write_obj("Start time","%lld", start_time_ms);
        jsonhandler->write_obj("Finish time","%lld", end_time_ms);
        jsonhandler->write_obj("Total duration","%lld", end_time_ms-start_time_ms);
        jsonhandler->write_obj("Time unit","\"%s\"","MILLISECONDS");
        jsonhandler->write_obj("Interrupted","\"%s\"", m_interrupted ? "true" : "false");
        jsonhandler->close_nesting();
    }
    std::vector<unsigned int> timestamps = get_one_sec_cmd_stats_timestamp();

    if (print_arbitrary_commands_results()) {
        for (unsigned int i=0; i<m_totals.m_ar_commands.size(); i++) {
            // format command name
            std::string command_name = command_list[i].command_name;

            std::transform(command_name.begin(), command_name.end(), command_name.begin(), ::tolower);
            command_name[0] = static_cast<char>(toupper(command_name[0]));
            command_name.append("s");
            struct hdr_histogram* arbitrary_command_latency_histogram = m_ar_commands_latency_histograms.at(i);
            std::vector<one_sec_cmd_stats> arbitrary_command_stats = get_one_sec_cmd_stats_arbitrary_command(i);

            result_print_to_json(jsonhandler, command_name.c_str(), m_totals.m_ar_commands[i].m_ops_sec,
                                 0.0,
                                 0.0,
                                 cluster_mode ? m_totals.m_ar_commands[i].m_moved_sec : -1,
                                 cluster_mode ? m_totals.m_ar_commands[i].m_ask_sec : -1,
                                 m_totals.m_ar_commands[i].m_bytes_sec,
                                 m_totals.m_ar_commands[i].m_bytes_sec_rx,
                                 m_totals.m_ar_commands[i].m_bytes_sec_tx,
                                 m_totals.m_ar_commands[i].m_latency,
                                 m_totals.m_ar_commands[i].m_total_latency,
                                 m_totals.m_ar_commands[i].m_ops,
                                 0.0, // connection_errors_sec (not tracked per command)
                                 0,   // connection_errors (not tracked per command)
                                 quantiles_list,
                                 arbitrary_command_latency_histogram,
                                 timestamps,
                                 arbitrary_command_stats
                                 );
        }
    } else {
        std::vector<one_sec_cmd_stats> get_stats = get_one_sec_cmd_stats_get();
        std::vector<one_sec_cmd_stats> set_stats = get_one_sec_cmd_stats_set();
        std::vector<one_sec_cmd_stats> wait_stats = get_one_sec_cmd_stats_wait();
        result_print_to_json(jsonhandler, "Sets",m_totals.m_set_cmd.m_ops_sec,
                             0.0,
                             0.0,
                             cluster_mode ? m_totals.m_set_cmd.m_moved_sec : -1,
                             cluster_mode ? m_totals.m_set_cmd.m_ask_sec : -1,
                             m_totals.m_set_cmd.m_bytes_sec,
                             m_totals.m_set_cmd.m_bytes_sec_rx,
                             m_totals.m_set_cmd.m_bytes_sec_tx,
                             m_totals.m_set_cmd.m_latency,
                             m_totals.m_set_cmd.m_total_latency,
                             m_totals.m_set_cmd.m_ops,
                             0.0, // connection_errors_sec (not tracked per command)
                             0,   // connection_errors (not tracked per command)
                             quantiles_list,
                             m_set_latency_histogram,
                             timestamps,
                             set_stats
                            );
        result_print_to_json(jsonhandler,"Gets",m_totals.m_get_cmd.m_ops_sec,
                             m_totals.m_hits_sec,
                             m_totals.m_misses_sec,
                             cluster_mode ? m_totals.m_get_cmd.m_moved_sec : -1,
                             cluster_mode ? m_totals.m_get_cmd.m_ask_sec : -1,
                             m_totals.m_get_cmd.m_bytes_sec,
                             m_totals.m_get_cmd.m_bytes_sec_rx,
                             m_totals.m_get_cmd.m_bytes_sec_tx,
                             m_totals.m_get_cmd.m_latency,
                             m_totals.m_get_cmd.m_total_latency,
                             m_totals.m_get_cmd.m_ops,
                             0.0, // connection_errors_sec (not tracked per command)
                             0,   // connection_errors (not tracked per command)
                             quantiles_list,
                             m_get_latency_histogram,
                             timestamps,
                             get_stats
                             );
        result_print_to_json(jsonhandler,"Waits",m_totals.m_wait_cmd.m_ops_sec,
                             0.0,
                             0.0,
                             cluster_mode ? 0.0 : -1,
                             cluster_mode ? 0.0 : -1,
                             0.0,
                             0.0,
                             0.0,
                             0.0,
                             0.0,
                             m_totals.m_wait_cmd.m_ops,
                             0.0, // connection_errors_sec (not tracked per command)
                             0,   // connection_errors (not tracked per command)
                             quantiles_list,
                             m_wait_latency_histogram,
                             timestamps,
                             wait_stats
                             );
    }
    std::vector<one_sec_cmd_stats> total_stats = get_one_sec_cmd_stats_totals();
    result_print_to_json(jsonhandler,"Totals",m_totals.m_ops_sec,
                         m_totals.m_hits_sec,
                         m_totals.m_misses_sec,
                         cluster_mode ? m_totals.m_moved_sec : -1,
                         cluster_mode ? m_totals.m_ask_sec : -1,
                         m_totals.m_bytes_sec,
                         m_totals.m_bytes_sec_rx,
                         m_totals.m_bytes_sec_tx,
                         m_totals.m_latency,
                         m_totals.m_total_latency,
                         m_totals.m_ops,
                         m_totals.m_connection_errors_sec,
                         m_totals.m_connection_errors,
                         quantiles_list,
                         m_totals.latency_histogram,
                         timestamps,
                         total_stats
                         );
}

void run_stats::print_histogram(FILE *out, json_handler *jsonhandler, arbitrary_command_list& command_list) {
    fprintf(out,
            "\n\n"
            "Request Latency Distribution\n"
            "%-6s %12s %12s\n"
            "------------------------------------------------------------------------\n",
            "Type", "<= msec   ", "Percent");
    struct hdr_iter iter;
    struct hdr_iter_percentiles * percentiles;
    

    if (print_arbitrary_commands_results()) {
        for (unsigned int i = 0; i < command_list.size(); i++) {
            std::string command_name = command_list[i].command_name;

            if (jsonhandler != NULL) { jsonhandler->open_nesting(command_name.c_str(), NESTED_ARRAY); }

            struct hdr_histogram* arbitrary_command_latency_histogram = m_ar_commands_latency_histograms.at(i);
            hdr_iter_percentile_init(&iter, arbitrary_command_latency_histogram, LATENCY_HDR_GRANULARITY);
            percentiles = &iter.specifics.percentiles;
            while (hdr_iter_next(&iter)){
                double value = iter.highest_equivalent_value / (double) LATENCY_HDR_RESULTS_MULTIPLIER;
                histogram_print(out, jsonhandler, command_name.c_str(), value,percentiles->percentile);
            }

            if (jsonhandler != NULL) { jsonhandler->close_nesting(); }
            fprintf(out, "---\n");
        }
    } else {
        // SETs
        // ----
        if (jsonhandler != NULL){ jsonhandler->open_nesting("SET",NESTED_ARRAY);}
        hdr_iter_percentile_init(&iter, m_set_latency_histogram, LATENCY_HDR_GRANULARITY);
        percentiles = &iter.specifics.percentiles;
        while (hdr_iter_next(&iter))
        {
            double  value = iter.highest_equivalent_value / (double) LATENCY_HDR_RESULTS_MULTIPLIER;
            histogram_print(out, jsonhandler, "SET", value,percentiles->percentile);
        }
        if (jsonhandler != NULL){ jsonhandler->close_nesting();}
        fprintf(out, "---\n");
        // GETs
        // ----
        if (jsonhandler != NULL){ jsonhandler->open_nesting("GET",NESTED_ARRAY);}
        hdr_iter_percentile_init(&iter, m_get_latency_histogram, LATENCY_HDR_GRANULARITY);
        percentiles = &iter.specifics.percentiles;
        while (hdr_iter_next(&iter))
        {
            double  value = iter.highest_equivalent_value / (double) LATENCY_HDR_RESULTS_MULTIPLIER;
            histogram_print(out, jsonhandler, "GET", value,percentiles->percentile);
        }
        if (jsonhandler != NULL){ jsonhandler->close_nesting();}
        fprintf(out, "---\n");
        // WAITs
        // ----
        if (jsonhandler != NULL){ jsonhandler->open_nesting("WAIT",NESTED_ARRAY);}
        hdr_iter_percentile_init(&iter, m_wait_latency_histogram, LATENCY_HDR_GRANULARITY);
        percentiles = &iter.specifics.percentiles;
        while (hdr_iter_next(&iter))
        {
            double  value               = iter.highest_equivalent_value / (double) LATENCY_HDR_RESULTS_MULTIPLIER;
            histogram_print(out, jsonhandler, "WAIT", value,percentiles->percentile);
        }
        if (jsonhandler != NULL){ jsonhandler->close_nesting();}
    }
}

void run_stats::print(FILE *out, benchmark_config *config,
                      const char * header/*=NULL*/,  json_handler * jsonhandler/*=NULL*/)
{
    // aggregate all one_second_stats; we do this only if we have
    // one_second_stats, otherwise it means we're probably printing previously
    // aggregated data
    if (m_stats.size() > 0 || m_cur_stats.m_connection_errors > 0) {
        summarize(m_totals);
    }

    output_table table;

    // Type column
    print_type_column(table, *config->arbitrary_commands);

    // Ops/sec column
    print_ops_sec_column(table);

    // Hits/sec column (not relevant for arbitrary commands)
    if (!print_arbitrary_commands_results()) {
        print_hits_sec_column(table);
    }

    // Misses/sec column (not relevant for arbitrary commands)
    if (!print_arbitrary_commands_results()) {
        print_missess_sec_column(table);
    }

    // Moved & ASK column (relevant only for cluster mode)
    if (config->cluster_mode) {
        print_moved_sec_column(table);
        print_ask_sec_column(table);
    }

    // Latency column
    print_avg_latency_column(table);

    for (std::size_t i = 0; i < config->print_percentiles.quantile_list.size(); i++){
        float quantile = config->print_percentiles.quantile_list[i];
        char average_header[50];
        int ndigts = 0;
        float num = abs(quantile);
        num = num - int(num);
        while (abs(num)>0.001 && ndigts < 3 ) {
            num = num * 10;
            ndigts++;
            num = num - int(num);
        }
        snprintf(average_header,sizeof(average_header)-1,"p%.*f Latency", ndigts, quantile);
        print_quantile_latency_column(table,quantile,(char *)average_header);
    }

    // KB/sec column
    print_kb_sec_column(table);

    // print results
    table.print(out, header);

    ////////////////////////////////////////
    // JSON print handling
    // ------------------
    if (jsonhandler != NULL) {

        if (header != NULL) {
            jsonhandler->open_nesting(header);
        } else {
            jsonhandler->open_nesting("UNKNOWN STATS");
        }

        print_json(jsonhandler, *config->arbitrary_commands, config->cluster_mode);
    }

    if (!config->hide_histogram) {
        print_histogram(out, jsonhandler, *config->arbitrary_commands);
    }

    // This close_nesting closes either:
    //      jsonhandler->open_nesting(header); or
    //      jsonhandler->open_nesting("UNKNOWN STATS");
    //      From the top (beginning of function).
    if (jsonhandler != NULL){ jsonhandler->close_nesting();}
}
