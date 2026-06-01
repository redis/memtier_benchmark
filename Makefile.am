# Makefile for memtier_benchmark
# Copyright (C) 2011-2026 Redis Labs Ltd.

# This file is part of memtier_benchmark.

# memtier_benchmark is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

AUTOMAKE_OPTIONS = foreign 1.9 subdir-objects
CODE_COVERAGE_IGNORE_PATTERN='*/deps/hdr_histogram/*'

EXTRA_DIST = README.md COPYING version.sh \
	scripts/fetch_commands_meta.sh \
	scripts/gen_command_meta.py \
	deps/commands_json/REDIS_TAG.txt

# Generate version.h before building
BUILT_SOURCES = version.h
CLEANFILES = version.h

version.h:
	$(SHELL) $(srcdir)/version.sh

bin_PROGRAMS = memtier_benchmark
completionsdir=$(BASH_COMPLETION_DIR)
completions_DATA=bash-completion/memtier_benchmark

memtier_benchmark_CPPFLAGS = \
	$(CODE_COVERAGE_CFLAGS) \
	$(LIBEVENT_CFLAGS) \
	$(LIBEVENT_PTHREADS_CFLAGS) \
	$(LIBEVENT_OPENSSL_CFLAGS) \
	$(LIBCRYPTO_CFLAGS) \
	$(LIBSSL_CFLAGS)
memtier_benchmark_SOURCES = \
	memtier_benchmark.cpp memtier_benchmark.h \
	client.cpp client.h \
	cluster_client.cpp cluster_client.h \
	shard_connection.cpp shard_connection.h connections_manager.h \
	run_stats_types.cpp run_stats_types.h \
	run_stats.cpp run_stats.h \
	JSON_handler.cpp JSON_handler.h \
	protocol.cpp protocol.h \
	obj_gen.cpp obj_gen.h \
	item.cpp item.h \
	file_io.cpp file_io.h \
	config_types.cpp config_types.h \
	command_meta.cpp command_meta.h command_meta_data.h \
	statsd.cpp statsd.h \
	retry_policy.cpp retry_policy.h \
	deps/hdr_histogram/hdr_histogram_log.c deps/hdr_histogram/hdr_histogram_log.h deps/hdr_histogram/byteorder.h \
	deps/hdr_histogram/hdr_histogram.c deps/hdr_histogram/hdr_histogram.h \
	deps/hdr_histogram/hdr_time.c deps/hdr_histogram/hdr_time.h deps/hdr_histogram/hdr_encoding.c deps/hdr_histogram/hdr_encoding.h

memtier_benchmark_LDADD = \
	$(CODE_COVERAGE_LIBS) \
	$(LIBEVENT_LIBS) \
	$(LIBEVENT_PTHREADS_LIBS) \
	$(LIBEVENT_OPENSSL_LIBS) \
	$(LIBCRYPTO_LIBS) \
	$(LIBSSL_LIBS)


@CODE_COVERAGE_RULES@
memtier_benchmark_LDFLAGS = $(CODE_COVERAGE_LDFLAGS)

dist_man1_MANS = memtier_benchmark.1

.PHONY: rebuild-man
rebuild-man:
	help2man --name="NoSQL benchmark tool" --no-info --no-discard-stderr --output=memtier_benchmark.1 $(builddir)/memtier_benchmark

# Code formatting
.PHONY: format format-check
format:
	@echo "Formatting C++ source files..."
	@find $(srcdir) -name "*.cpp" -o -name "*.h" | grep -v deps/ | grep -v autom4te.cache/ | xargs clang-format -i
	@echo "Done."

format-check:
	@echo "Checking C++ source file formatting..."
	@FAILED=0; \
	for file in $$(find $(srcdir) -name "*.cpp" -o -name "*.h" | grep -v deps/ | grep -v autom4te.cache/); do \
		if ! clang-format --dry-run --Werror "$$file" 2>/dev/null; then \
			echo "Needs formatting: $$file"; \
			FAILED=1; \
		fi; \
	done; \
	if [ $$FAILED -eq 1 ]; then \
		echo ""; \
		echo "Some files need formatting. Run: make format"; \
		exit 1; \
	fi; \
	echo "All C++ files are properly formatted!"

# Like format-check, but inspects the staged blobs in the git index rather than
# the working tree. Used by the pre-commit hook so that:
#  - mis-formatted *staged* content cannot slip past when `make format` was run
#    against the working tree but the result was never re-staged, and
#  - unrelated mis-formatted files lying in the working tree do not block an
#    otherwise-clean commit.
.PHONY: format-check-staged
format-check-staged:
	@STAGED=$$(git diff --cached --name-only --diff-filter=ACMR \
	            | grep -E '\.(cpp|cc|cxx|h|hpp|hxx)$$' \
	            | grep -v '^deps/' || true); \
	if [ -z "$$STAGED" ]; then \
		exit 0; \
	fi; \
	echo "Checking staged C/C++ source file formatting..."; \
	FAILED=0; \
	for file in $$STAGED; do \
		if ! git show ":$$file" | clang-format --assume-filename="$$file" --dry-run --Werror 2>/dev/null; then \
			echo "Needs formatting (staged): $$file"; \
			FAILED=1; \
		fi; \
	done; \
	if [ $$FAILED -eq 1 ]; then \
		echo ""; \
		echo "Some staged files need formatting. Stage the formatted version:"; \
		echo "    make format && git add -u"; \
		exit 1; \
	fi; \
	echo "All staged C/C++ files are properly formatted!"

# Git hooks
.PHONY: install-hooks
install-hooks:
	@git config core.hooksPath .githooks
	@echo "Git hooks enabled (core.hooksPath = .githooks)."
	@echo "Pre-commit hook will run 'make format-check-staged' on staged C/C++ changes."

# Command metadata vendored from redis/redis@<REDIS_VERSION>.
# This is a maintainer step, not invoked by `make all`. It refreshes the
# vendored src/commands/*.json snapshot in deps/commands_json/ and regenerates
# the command_meta_data.h header consumed by command_meta.cpp.
#
# Usage:
#   make commands-meta                       # uses default pinned version
#   make commands-meta REDIS_VERSION=8.8     # override branch / tag / sha
#
# The two halves can be invoked independently for partial regeneration:
#   make commands-meta-fetch REDIS_VERSION=unstable
#   make commands-meta-gen   # rebuild .h from already-vendored JSONs
#
# CI guard:
#   make commands-meta-check                 # fails if .h is stale
REDIS_VERSION ?= 8.8

.PHONY: commands-meta commands-meta-fetch commands-meta-gen commands-meta-check
commands-meta: commands-meta-fetch commands-meta-gen
	@echo "commands-meta: refreshed for redis $(REDIS_VERSION)"

commands-meta-fetch:
	$(SHELL) $(srcdir)/scripts/fetch_commands_meta.sh $(REDIS_VERSION) $(srcdir)/deps/commands_json

commands-meta-gen:
	$(PYTHON) $(srcdir)/scripts/gen_command_meta.py \
	    --in  $(srcdir)/deps/commands_json \
	    --out $(srcdir)/command_meta_data.h
	@if command -v clang-format >/dev/null 2>&1; then \
	    clang-format -i $(srcdir)/command_meta_data.h; \
	fi

commands-meta-check:
	@TMP=$$(mktemp); \
	$(PYTHON) $(srcdir)/scripts/gen_command_meta.py \
	    --in  $(srcdir)/deps/commands_json \
	    --out $$TMP > /dev/null; \
	if ! diff -u $(srcdir)/command_meta_data.h $$TMP; then \
	    echo ""; \
	    echo "command_meta_data.h is stale; run 'make commands-meta-gen'"; \
	    rm -f $$TMP; exit 1; \
	fi; \
	rm -f $$TMP; \
	echo "command_meta_data.h is up to date."

# ----------------------------------------------------------------------------
# libFuzzer harnesses for in-process parsers (--enable-fuzz).
#
# These are noinst_PROGRAMS — they're built only on demand and never installed.
# We re-compile config_types.cpp / protocol.cpp / command_meta.cpp into the
# fuzz target rather than reusing the memtier_benchmark objects so the
# fuzz-only sanitizer flags (-fsanitize=fuzzer,address,undefined) don't leak
# into the main binary's build.
#
# A tiny shim (fuzz_stubs.cpp) provides the logging entry points expected by
# protocol.cpp (defined in memtier_benchmark.cpp in the real build) so we
# don't have to drag main() and the entire async stack in.
# ----------------------------------------------------------------------------
if FUZZ_ENABLED

memtier_fuzz_cxxflags   = -fsanitize=fuzzer,address,undefined -fno-omit-frame-pointer -g -O1 -std=c++11 -Wall \
                  -I$(srcdir) $(LIBEVENT_CFLAGS)
memtier_fuzz_ldflags    = -fsanitize=fuzzer,address,undefined
memtier_fuzz_ldadd      = $(LIBEVENT_LIBS) -lpthread -lz

memtier_fuzz_shared_sources = \
    config_types.cpp config_types.h \
    protocol.cpp protocol.h \
    command_meta.cpp command_meta.h command_meta_data.h \
    tests/fuzz/fuzz_stubs.cpp

noinst_PROGRAMS = \
    split_command_to_args_fuzz \
    monitor_input_fuzz \
    redis_protocol_fuzz

split_command_to_args_fuzz_SOURCES = \
    tests/fuzz/split_command_to_args_fuzz.cpp \
    $(memtier_fuzz_shared_sources)
split_command_to_args_fuzz_CPPFLAGS = $(memtier_fuzz_cxxflags)
split_command_to_args_fuzz_CXXFLAGS = $(memtier_fuzz_cxxflags)
split_command_to_args_fuzz_LDFLAGS  = $(memtier_fuzz_ldflags)
split_command_to_args_fuzz_LDADD    = $(memtier_fuzz_ldadd)

monitor_input_fuzz_SOURCES = \
    tests/fuzz/monitor_input_fuzz.cpp \
    $(memtier_fuzz_shared_sources)
monitor_input_fuzz_CPPFLAGS = $(memtier_fuzz_cxxflags)
monitor_input_fuzz_CXXFLAGS = $(memtier_fuzz_cxxflags)
monitor_input_fuzz_LDFLAGS  = $(memtier_fuzz_ldflags)
monitor_input_fuzz_LDADD    = $(memtier_fuzz_ldadd)

redis_protocol_fuzz_SOURCES = \
    tests/fuzz/redis_protocol_fuzz.cpp \
    $(memtier_fuzz_shared_sources)
redis_protocol_fuzz_CPPFLAGS = $(memtier_fuzz_cxxflags)
redis_protocol_fuzz_CXXFLAGS = $(memtier_fuzz_cxxflags)
redis_protocol_fuzz_LDFLAGS  = $(memtier_fuzz_ldflags)
redis_protocol_fuzz_LDADD    = $(memtier_fuzz_ldadd)

endif
