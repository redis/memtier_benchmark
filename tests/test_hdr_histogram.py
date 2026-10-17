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


def _v2_counts(raw):
    """Independently decode bucket counts using the geometry in the V2 header."""
    header_format = ">IIiiqqd"
    header_size = struct.calcsize(header_format)
    _, _, offset, digits, lowest, _, _ = struct.unpack(header_format, raw[:header_size])
    assert offset == 0 and lowest > 0
    unit_magnitude = lowest.bit_length() - 1
    half_magnitude = (2 * 10 ** digits - 1).bit_length() - 1
    counts, index, word, shift = {}, 0, 0, 0
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
                counts[index] = count
            index += 1
    return (unit_magnitude, half_magnitude), counts


def _merge_counts(*decoded):
    geometry, merged = decoded[0][0], {}
    for other_geometry, counts in decoded:
        assert other_geometry == geometry
        for index, count in counts.items():
            merged[index] = merged.get(index, 0) + count
    return geometry, merged


def _percentile(decoded, percentile):
    """Rank lookup matching hdr_value_at_percentile over decoded bucket counts."""
    (unit_magnitude, half_magnitude), counts = decoded
    half_count = 1 << half_magnitude
    rank = max(1, int(sum(counts.values()) * percentile / 100 + 0.5))
    cumulative = 0
    for index in sorted(counts):
        cumulative += counts[index]
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
    """Unsorted p0/p100/deep-tail queries keep table order and exact HDR ranks.

    Totals are checked against the merge of the independently decoded Sets and
    Gets histograms. Unpipelined large SETs and missing-key GETs give the rows
    distinct latencies, and a dense percentile grid crosses the boundary of the
    1:3 mix, so a missing, doubled or misplaced row in the totals merge changes
    a value.
    """
    # Unsorted on purpose: p0/p100/deep-tail first, then a reversed grid.
    requested = [100, 0, 99.99999, 99.9, 99] + list(range(95, 0, -5))
    cases = [
        ["--ratio=1:3"],
        ["--ratio=1:0"],
        ["--command=SET __key__ __data__", "--command=GET __key__", "--command-ratio=3"],
    ]
    for case in cases:
        with tempfile.TemporaryDirectory() as directory:
            specs = {"name": env.testName, "args": case + [
                "--pipeline=1", "--data-size=20000", "--key-maximum=1000000000",
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
                lines = source.read().splitlines()
            rows = {line.split()[0]: line.split() for line in lines
                    if line.startswith(("Sets ", "Gets ", "Totals "))}
            header = [line for line in lines if line.startswith("Type ")][-1].split()
            env.assertEqual([field for field in header if field.startswith("p")],
                            ["p%.10g" % p for p in requested])
            decoded = {}
            for name in ["Sets", "Gets", "Totals"]:
                encoded = stats[name]["Percentile Latencies"]["Histogram log format"]["Compressed Histogram"]
                count, raw = _decode_v2(encoded)
                env.assertEqual(count, stats[name]["Count"])
                decoded[name] = _v2_counts(raw)
            merged = _merge_counts(decoded["Sets"], decoded["Gets"])
            env.assertEqual(decoded["Totals"], merged)
            env.assertTrue(stats["Sets"]["Count"] > 0)
            for name in ["Sets", "Gets", "Totals"]:
                percentiles = stats[name]["Percentile Latencies"]
                actual = {float(key[1:]): value for key, value in percentiles.items() if key.startswith("p")}
                source = merged if name == "Totals" else decoded[name]
                expected = [_percentile(source, p) / 1000 for p in requested]
                env.assertEqual([actual[p] for p in requested], expected)
                # The last field is KB/sec, preceded by configured percentile columns.
                table_values = rows[name][-len(requested)-1:-1]
                if stats[name]["Count"] == 0 and name != "Totals":
                    env.assertEqual(table_values, ["---"] * len(requested))
                else:
                    env.assertEqual(list(map(float, table_values)), expected)


def test_aggregated_average_totals_histogram(env):
    """--run-count AVERAGE Totals must carry the merged totals histogram.

    Regression for #567: aggregate_average() merged the per-type histograms but
    not the totals histogram the JSON Totals block prints from, so the AVERAGE
    Totals percentiles came from an empty histogram while Count was right.
    """
    requested = [0, 50, 99, 100]
    with tempfile.TemporaryDirectory() as directory:
        specs = {"name": env.testName, "args": [
            "--ratio=1:3", "--pipeline=1", "--key-maximum=1000", "--run-count=2", "--print-all-runs",
            "--hide-histogram", "--print-percentiles=" + ",".join(map(str, requested)),
        ]}
        addTLSArgs(specs, env)
        config = get_default_memtier_config(threads=2, clients=2, requests=1000)
        add_required_env_arguments(specs, config, env, env.getMasterNodesList())
        config = RunConfig(directory, env.testName, config, {})
        ensure_clean_benchmark_folder(config.results_dir)
        env.assertTrue(Benchmark.from_json(config, specs).run())
        with open(os.path.join(config.results_dir, "mb.json")) as source:
            results = json.load(source)
        with open(os.path.join(config.results_dir, "mb.stdout")) as source:
            lines = source.read().splitlines()

        def decode(section, name, runs=1):
            stats = results[section][name]
            encoded = stats["Percentile Latencies"]["Histogram log format"]["Compressed Histogram"]
            count, raw = _decode_v2(encoded)
            # The AVERAGE histogram holds the samples of every run, while its
            # Count is the per-run mean (#703).
            env.assertEqual(count, runs * stats["Count"])
            return _v2_counts(raw)

        average = "AGGREGATED AVERAGE RESULTS (2 runs)"
        totals = results[average]["Totals"]
        env.assertEqual(totals["Count"], 4000)
        decoded = decode(average, "Totals", 2)
        env.assertEqual(decoded, _merge_counts(decode(average, "Sets", 2), decode(average, "Gets", 2)))
        env.assertEqual(decoded, _merge_counts(decode("RUN #1 RESULTS", "Totals"),
                                               decode("RUN #2 RESULTS", "Totals")))

        expected = [_percentile(decoded, p) / 1000 for p in requested]
        actual = {float(key[1:]): value for key, value in totals["Percentile Latencies"].items()
                  if key.startswith("p")}
        env.assertEqual([actual[p] for p in requested], expected)
        env.assertEqual(totals["Min Latency"], expected[0])
        env.assertEqual(totals["Max Latency"], expected[-1])
        # The AVERAGE table's Totals row is printed after its section header.
        start = max(i for i, line in enumerate(lines) if average in line)
        row = next(line.split() for line in lines[start:] if line.startswith("Totals "))
        env.assertEqual(list(map(float, row[-len(requested)-1:-1])), expected)


def _hgrm_count(filename):
    with open(filename) as source:
        records = [line.strip() for line in source
                   if line.strip() and not line.startswith(("#", '"'))]
    assert len(records) == 1
    return _decode_v2(records[0].split(",")[-1])[0]


def test_hdr_files_for_duplicate_command_names(env):
    """Two --command lines with the same name must get distinct HDR files.

    Regression for #587: both GET lines wrote <prefix>_GET_command_run_1.*,
    so the second overwrote the first and one line's histogram was lost.
    Repeated names get a shell-safe "_N" suffix (the JSON keys use " #N",
    but an unquoted "#" starts a shell comment); unique names keep their
    usual file name.
    """
    with tempfile.TemporaryDirectory() as directory:
        prefix = os.path.join(directory, "histogram")
        specs = {"name": env.testName, "args": [
            # Different ratios give the two GET lines different sample counts,
            # so an overwritten file cannot pass for the right one.
            "--command=GET __key__", "--command-ratio=3",
            "--command=GET __key__",
            "--command=SET __key__ __data__",
            "--hide-histogram", "--hdr-file-prefix=" + prefix,
        ]}
        addTLSArgs(specs, env)
        config = get_default_memtier_config(threads=1, clients=1, requests=100)
        add_required_env_arguments(specs, config, env, env.getMasterNodesList())
        config = RunConfig(directory, env.testName, config, {})
        ensure_clean_benchmark_folder(config.results_dir)
        env.assertTrue(Benchmark.from_json(config, specs).run())

        # 100 requests in rounds of 3 + 1 + 1.
        expected = {"GET_1": 60, "GET_2": 20, "SET": 20}
        for name, count in expected.items():
            base = "{}_{}_command_run_1".format(prefix, name)
            env.assertTrue(os.path.exists(base + ".txt"))
            env.assertTrue(os.path.exists(base + ".hgrm"))
            if os.path.exists(base + ".hgrm"):
                env.assertEqual(_hgrm_count(base + ".hgrm"), count)
        env.assertFalse(os.path.exists(prefix + "_GET_command_run_1.hgrm"))
        names = os.listdir(directory)
        env.assertEqual([n for n in names if "#" in n or " " in n], [])


# Characters that are reserved in Windows file names or rejected by
# actions/upload-artifact, plus space and ASCII control characters.
_UNSAFE_FILE_NAME_CHARS = set('<>:"/\\|?* ') | {chr(c) for c in range(0x20)} | {"\x7f"}


def _unsafe_hdr_file_names(prefix):
    # Only the HDR outputs; the RLTest results dir is named after the test.
    directory, base = os.path.split(prefix)
    return sorted(name for name in os.listdir(directory)
                  if name.startswith(base + "_") and _UNSAFE_FILE_NAME_CHARS & set(name))


def _run_hdr_benchmark(env, directory, args, requests):
    prefix = os.path.join(directory, "histogram")
    specs = {"name": env.testName,
             "args": args + ["--hide-histogram", "--hdr-file-prefix=" + prefix]}
    addTLSArgs(specs, env)
    config = get_default_memtier_config(threads=1, clients=1, requests=requests)
    add_required_env_arguments(specs, config, env, env.getMasterNodesList())
    config = RunConfig(directory, env.testName, config, {})
    ensure_clean_benchmark_folder(config.results_dir)
    env.assertTrue(Benchmark.from_json(config, specs).run())
    return prefix


def test_hdr_file_names_are_sanitized_for_scan_cursor(env):
    """HDR file names must not contain characters unsafe in file names.

    Regression for #627: --scan-incremental-iteration names its rows
    "SCAN 0" and "SCAN <cursor>", which produced
    <prefix>_SCAN <cursor>_command_run_1.hgrm. '<' and '>' are reserved
    on Windows and rejected by actions/upload-artifact.
    """
    env.skipOnCluster()
    # More keys than one SCAN page, so continuation SCANs are sent and the
    # "SCAN <cursor>" histogram is non-empty (empty ones are not written).
    conn = env.getConnection()
    keys = ["hdrname:{}".format(i) for i in range(200)]
    conn.mset({key: "v" for key in keys})
    try:
        with tempfile.TemporaryDirectory() as directory:
            prefix = _run_hdr_benchmark(env, directory, [
                "--command=SCAN 0 COUNT 10", "--scan-incremental-iteration",
            ], requests=50)
            env.assertEqual(_unsafe_hdr_file_names(prefix), [])
            for name in ("SCAN_0", "SCAN__cursor_"):
                base = "{}_{}_command_run_1".format(prefix, name)
                env.assertTrue(os.path.exists(base + ".txt"))
                env.assertTrue(os.path.exists(base + ".hgrm"))
    finally:
        conn.delete(*keys)


def test_hdr_file_names_stay_unique_after_sanitizing(env):
    """Names that sanitize to the same string must still get distinct files.

    "A:" and "A?" both sanitize to "A_", so they get the duplicate "_N"
    suffix (#587). The literal name "A__1" already uses the first suffix,
    so the sanitized duplicates must skip it instead of overwriting its file.
    'A/\\|*"x"' covers the remaining reserved characters: it must sanitize
    to "A_____X_" and keep its own histogram.
    """
    with tempfile.TemporaryDirectory() as directory:
        prefix = _run_hdr_benchmark(env, directory, [
            # Different ratios give each line a different sample count, so an
            # overwritten file cannot pass for the right one.
            "--command=A: __key__", "--command-ratio=3",
            "--command=A? __key__",
            "--command=A__1 __key__",
            # The quotes only wrap "x" for the argument parser; the display
            # name keeps them, so it contains / \ | * and ".
            '--command=A/\\|*"x" __key__',
        ], requests=120)
        env.assertEqual(_unsafe_hdr_file_names(prefix), [])
        # 120 requests in rounds of 3 + 1 + 1 + 1.
        expected = {"A__2": 60, "A__3": 20, "A__1": 20, "A_____X_": 20}
        for name, count in expected.items():
            base = "{}_{}_command_run_1".format(prefix, name)
            env.assertTrue(os.path.exists(base + ".txt"))
            env.assertTrue(os.path.exists(base + ".hgrm"))
            if os.path.exists(base + ".hgrm"):
                env.assertEqual(_hgrm_count(base + ".hgrm"), count)
