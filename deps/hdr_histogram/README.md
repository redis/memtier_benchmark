# HdrHistogram_c 0.12.0

Vendored from the upstream generated core-plus-log release:
https://github.com/HdrHistogram/HdrHistogram_c/releases/tag/0.12.0

- Source commit: `8885476fc83fa362fec2fb2b5e9cd9544514976e`.
- Asset: `hdr_histogram-amalgamation-with-log-0.12.0.zip`.
- Asset SHA-256: `5fae248faedfcae85731530ecabe2cd485810b7b33055b30fce97ad7638e3527`.
- `SHA256SUMS` contains the upstream hashes of the eight vendored C/header files.
  Verify them from this directory with `sha256sum -c SHA256SUMS` (Linux) or
  `shasum -a 256 -c SHA256SUMS` (macOS).

These files are unmodified upstream output. To regenerate them from the pinned
source checkout, run `python3 script/amalgamate.py --output <directory> --with-log`.
The core/log amalgamation includes neither the packed histogram nor the interval
recorder. Packed storage is not enabled by this dependency upgrade.

Memtier's latency policy is in `../../latency_histogram.h`: values below the
configured minimum are raised before calling upstream's capped record helpers.
Upstream otherwise permits zero. The former local batch-percentile and
`hdr_total_count` helpers now use their upstream implementations, and JSON
histogram export uses `hdr_log_encode` (the standard V2 compressed/base64 format).

The upstream source is released into the public domain under CC0:
https://creativecommons.org/publicdomain/zero/1.0/
Each generated file retains its original attribution.
