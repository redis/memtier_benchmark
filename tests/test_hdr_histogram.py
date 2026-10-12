"""Validate JSON and HDR log exports using the standard V2 wire format."""
import base64
import json
import os
import struct
import tempfile
import zlib

from include import (get_default_memtier_config, add_required_env_arguments,
                     addTLSArgs, ensure_clean_benchmark_folder)
from mb import Benchmark, RunConfig


def _decode_v2(encoded):
    compressed = base64.b64decode(encoded, validate=True)
    cookie, length = struct.unpack(">II", compressed[:8])
    assert cookie & ~0xf0 == 0x1c849304
    assert length == len(compressed) - 8
    raw = zlib.decompress(compressed[8:])
    cookie, length, offset, digits, lowest, highest, ratio = struct.unpack(">IIiiqqd", raw[:40])
    assert cookie & ~0xf0 == 0x1c849303
    assert (offset, digits, lowest, highest, ratio) == (0, 2, 10, 600000000, 1.0)
    payload = raw[40:]
    assert len(payload) == length
    count, word, shift = 0, 0, 0
    for byte in payload:
        word |= (byte & 0x7f) << shift
        if byte & 0x80:
            shift += 7
            assert shift < 70
            continue
        value = (word >> 1) ^ -(word & 1)
        if value > 0:
            count += value
        # Negative words are runs of zero-count buckets.
        word, shift = 0, 0
    assert shift == 0
    return count, raw


def test_hdr_json_and_log_exports_agree(env):
    with tempfile.TemporaryDirectory() as directory:
        prefix = os.path.join(directory, "histogram")
        specs = {"name": env.testName, "args": [
            "--ratio=1:1", "--pipeline=4", "--key-maximum=1000",
            "--print-percentiles=50,99,99.9,100", "--hdr-file-prefix=" + prefix,
        ]}
        addTLSArgs(specs, env)
        config = get_default_memtier_config(threads=2, clients=2, requests=1000)
        add_required_env_arguments(specs, config, env, env.getMasterNodesList())
        config = RunConfig(directory, env.testName, config, {})
        ensure_clean_benchmark_folder(config.results_dir)
        env.assertTrue(Benchmark.from_json(config, specs).run())
        with open(os.path.join(config.results_dir, "mb.json")) as source:
            stats = json.load(source)["ALL STATS"]
        env.assertEqual(stats["Totals"]["Count"], 4000)
        for name, suffix in [("Sets", "SET_command"), ("Gets", "GET_command"), ("Totals", "FULL_RUN")]:
            percentiles = stats[name]["Percentile Latencies"]
            encoded = percentiles["Histogram log format"]["Compressed Histogram"]
            count, payload = _decode_v2(encoded)
            env.assertEqual(count, stats[name]["Count"])
            env.assertTrue(count > 0)
            if suffix == "FULL_RUN":
                filename = prefix + "_FULL_RUN_1.hgrm"
            else:
                filename = prefix + "_" + suffix + "_run_1.hgrm"
            with open(filename) as source:
                records = [line.strip() for line in source
                           if line.strip() and not line.startswith(("#", '"'))]
            env.assertEqual(len(records), 1)
            log_count, log_payload = _decode_v2(records[0].split(",")[-1])
            env.assertEqual(log_count, count)
            env.assertEqual(log_payload, payload)


def _percentile_from_v2(raw, percentile):
    """Independent rank lookup using the geometry recorded in the V2 header."""
    header_format = ">IIiiqqd"
    header_size = struct.calcsize(header_format)
    _, _, offset, digits, lowest, _, _ = struct.unpack(header_format, raw[:header_size])
    assert offset == 0 and lowest > 0
    unit_magnitude = lowest.bit_length() - 1
    half_magnitude = (2 * 10 ** digits - 1).bit_length() - 1
    half_count = 1 << half_magnitude
    counts, index, word, shift = [], 0, 0, 0
    for byte in raw[header_size:]:
        word |= (byte & 0x7f) << shift
        if byte & 0x80:
            shift += 7
            continue
        count = (word >> 1) ^ -(word & 1)
        word, shift = 0, 0
        if count < 0:
            index -= count
        else:
            if count:
                counts.append((index, count))
            index += 1
    rank = max(1, int(sum(count for _, count in counts) * percentile / 100 + 0.5))
    cumulative = 0
    for index, count in counts:
        cumulative += count
        if cumulative < rank:
            continue
        bucket = (index >> half_magnitude) - 1
        sub_bucket = (index & (half_count - 1)) + half_count
        if bucket < 0:
            bucket, sub_bucket = 0, sub_bucket - half_count
        low = sub_bucket << (bucket + unit_magnitude)
        return low if percentile == 0 else low + (1 << (bucket + unit_magnitude)) - 1
    return 0 if percentile == 0 else (1 << unit_magnitude) - 1


def test_percentile_values_match_encoded_distribution_and_table(env):
    """Unsorted p0/p100/deep-tail queries keep table order and exact HDR ranks."""
    requested = [100, 0, 99.99999, 50, 99, 99.9]
    for ratio in ["1:1", "1:0"]:
        with tempfile.TemporaryDirectory() as directory:
            specs = {"name": env.testName, "args": [
                "--ratio=" + ratio, "--pipeline=4", "--key-maximum=1000",
                "--hide-histogram", "--print-percentiles=" + ",".join(map(str, requested)),
            ]}
            addTLSArgs(specs, env)
            config = get_default_memtier_config(threads=2, clients=2, requests=1000)
            add_required_env_arguments(specs, config, env, env.getMasterNodesList())
            config = RunConfig(directory, env.testName, config, {})
            ensure_clean_benchmark_folder(config.results_dir)
            env.assertTrue(Benchmark.from_json(config, specs).run())
            with open(os.path.join(config.results_dir, "mb.json")) as source:
                stats = json.load(source)["ALL STATS"]
            with open(os.path.join(config.results_dir, "mb.stdout")) as source:
                rows = {line.split()[0]: line.split() for line in source
                        if line.startswith(("Sets ", "Gets ", "Totals "))}
            for name in ["Sets", "Gets", "Totals"]:
                percentiles = stats[name]["Percentile Latencies"]
                encoded = percentiles["Histogram log format"]["Compressed Histogram"]
                count, raw = _decode_v2(encoded)
                env.assertEqual(count, stats[name]["Count"])
                actual = {float(key[1:]): value for key, value in percentiles.items() if key.startswith("p")}
                expected = [_percentile_from_v2(raw, p) / 1000 for p in requested]
                env.assertEqual([actual[p] for p in requested], expected)
                # The last field is KB/sec, preceded by configured percentile columns.
                table_values = rows[name][-len(requested)-1:-1]
                if count == 0 and name != "Totals":
                    env.assertEqual(table_values, ["---"] * len(requested))
                else:
                    env.assertEqual(list(map(float, table_values)), expected)
