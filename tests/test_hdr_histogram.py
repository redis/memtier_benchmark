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
