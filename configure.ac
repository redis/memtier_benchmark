dnl configure.ac for memtier_benchmark
dnl Copyright (C) 2011-2026 Redis Labs Ltd.

dnl This file is part of memtier_benchmark.

dnl memtier_benchmark is free software: you can redistribute it and/or modify
dnl it under the terms of the GNU General Public License as published by
dnl the Free Software Foundation, version 2.

dnl This program is distributed in the hope that it will be useful,
dnl but WITHOUT ANY WARRANTY; without even the implied warranty of
dnl MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
dnl GNU General Public License for more details.

dnl You should have received a copy of the GNU General Public License
dnl along with this program.  If not, see <http://www.gnu.org/licenses/>.

AC_PREREQ(2.59)
AC_INIT(memtier_benchmark,255.255.255,oss@redis.com)
AC_CONFIG_SRCDIR([memtier_benchmark.cpp])
AC_CONFIG_HEADER([config.h])
AM_INIT_AUTOMAKE
AM_SILENT_RULES([yes])
AC_CONFIG_MACRO_DIR([m4])
LT_INIT

# Python is only required for the maintainer-only `make commands-meta` target.
# Resolve to ":" (the no-op shell builtin) when absent so end-user builds keep
# working without Python installed.
AM_PATH_PYTHON([3.6],, [:])

dnl -Werror=vla rejects variable-length arrays at compile time. VLAs on the
dnl worker-thread stack are how PR #405 / issue #404 shipped: an input-sized
dnl `char buffer[N]` blew an 8-12 MB stack the moment a 20 MB MONITOR-replay
dnl line was selected. Keep this hard-error to prevent the next one.
CXXFLAGS="-O2 -g -Wall -Werror=vla -std=c++11 $CXXFLAGS"

# Checks for programs.
AC_PROG_CXX

# Coverage
AX_CODE_COVERAGE

# Checks for header files.
AC_HEADER_STDC
AC_HEADER_DIRENT
AC_CHECK_HEADERS([stdlib.h string.h sys/time.h getopt.h limits.h malloc.h stdlib.h unistd.h utime.h assert.h sys/socket.h sys/types.h])
AC_CHECK_HEADERS([fcntl.h netinet/tcp.h])
AC_CHECK_HEADERS([execinfo.h])
AC_CHECK_HEADERS([pthread.h])
AC_CHECK_HEADERS([zlib.h])
AC_CHECK_HEADERS([event2/event.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STAT
AC_TYPE_SIZE_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_CHECK_FUNCS([gettimeofday memchr memset socket strerror random_r drand48])

# TLS support is optional.
AC_ARG_ENABLE([tls],
  [AS_HELP_STRING([--disable-tls],
                  [Disable TLS/SSL support])])
AS_IF([test "x$enable_tls" != "xno"], [
       AC_DEFINE([USE_TLS],
                 [1],
                 [define to enable TLS/SSL support])
       PKG_CHECK_MODULES([LIBEVENT_OPENSSL],
                         [libevent_openssl >= 2.0.10],
                         AC_SUBST(LIBEVENT_OPENSSL_CFLAGS) AC_SUBST(LIBEVENT_OPENSSL_LIBS))
       PKG_CHECK_MODULES([LIBSSL],
                         [libssl],
                         AC_SUBST(LIBSSL_CFLAGS) AC_SUBST(LIBSSL_LIBS))
       PKG_CHECK_MODULES([LIBCRYPTO],
                         [libcrypto],
                         AC_SUBST(LIBCRYPTO_CFLAGS) AC_SUBST(LIBCRYPTO_LIBS))
       ], [])

# Sanitizers support (ASAN/LSAN) is optional.
AC_ARG_ENABLE([sanitizers],
  [AS_HELP_STRING([--enable-sanitizers],
                  [Enable AddressSanitizer and LeakSanitizer for memory error detection])])
AS_IF([test "x$enable_sanitizers" = "xyes"], [
       AC_MSG_NOTICE([Enabling AddressSanitizer and LeakSanitizer])
       CXXFLAGS="$CXXFLAGS -fsanitize=address -fsanitize=leak -fno-omit-frame-pointer -O1"
       LDFLAGS="$LDFLAGS -fsanitize=address -fsanitize=leak"
       ], [])

# UndefinedBehaviorSanitizer (UBSan) is optional and can be combined with ASAN.
AC_ARG_ENABLE([ubsan],
  [AS_HELP_STRING([--enable-ubsan],
                  [Enable UndefinedBehaviorSanitizer for undefined behavior detection])])
AS_IF([test "x$enable_ubsan" = "xyes"], [
       AC_MSG_NOTICE([Enabling UndefinedBehaviorSanitizer])
       CXXFLAGS="$CXXFLAGS -fsanitize=undefined -fno-omit-frame-pointer"
       LDFLAGS="$LDFLAGS -fsanitize=undefined"
       ], [])

# Thread Sanitizer (TSAN) is optional and mutually exclusive with ASAN.
AC_ARG_ENABLE([thread-sanitizer],
  [AS_HELP_STRING([--enable-thread-sanitizer],
                  [Enable ThreadSanitizer for data race detection])])
AS_IF([test "x$enable_thread_sanitizer" = "xyes"], [
       AS_IF([test "x$enable_sanitizers" = "xyes"], [
              AC_MSG_ERROR([--enable-thread-sanitizer and --enable-sanitizers are mutually exclusive])
       ])
       AC_MSG_NOTICE([Enabling ThreadSanitizer])
       CXXFLAGS="$CXXFLAGS -fsanitize=thread -fno-omit-frame-pointer -O1"
       LDFLAGS="$LDFLAGS -fsanitize=thread"
       ], [])

# libFuzzer-based in-process fuzz harnesses (requires clang++).
# When enabled, builds tests/fuzz/*_fuzz binaries under noinst_PROGRAMS.
# Does not perturb the main memtier_benchmark binary's build flags.
AC_ARG_ENABLE([fuzz],
  [AS_HELP_STRING([--enable-fuzz],
                  [Build libFuzzer harnesses under tests/fuzz/ (requires clang++)])])
AS_IF([test "x$enable_fuzz" = "xyes"], [
       AC_MSG_CHECKING([whether $CXX is clang++])
       AS_CASE(["`$CXX --version 2>/dev/null | head -n 1`"],
               [*clang*], [AC_MSG_RESULT([yes])],
               [AC_MSG_RESULT([no])
                AC_MSG_ERROR([--enable-fuzz requires clang++; re-run configure with CXX=clang++ CC=clang])])
       AC_MSG_NOTICE([Enabling libFuzzer fuzz harnesses (tests/fuzz/)])
       ], [])
AM_CONDITIONAL([FUZZ_ENABLED], [test "x$enable_fuzz" = "xyes"])

# clock_gettime requires -lrt on old glibc only.
AC_SEARCH_LIBS([clock_gettime], [rt], , AC_MSG_ERROR([rt is required libevent.]))


AC_CHECK_LIB([z], [deflateInit_], , AC_MSG_ERROR([zlib is required; try installing zlib1g-dev.]))
AC_CHECK_LIB([pthread], [pthread_create], , AC_MSG_ERROR([pthread is required.]))
AC_CHECK_LIB([socket], [gai_strerror])

# libevent
PKG_CHECK_MODULES(LIBEVENT,
    [libevent >= 2.0.10],
    AC_SUBST(LIBEVENT_CFLAGS) AC_SUBST(LIBEVENT_LIBS)
)

# libevent_pthreads: needed for evthread_use_pthreads(). Without this,
# event_base_loopbreak() / event_base_loopexit() called from a *different*
# thread than the one running event_base_dispatch() do not wake the
# epoll_wait(), so an idle worker hangs after a cross-thread shutdown
# request (Phase 1 of #426 / Ctrl+C). The pthreads bindings ship as a
# separate .so since libevent 2.0, so we have to depend on it explicitly.
PKG_CHECK_MODULES(LIBEVENT_PTHREADS,
    [libevent_pthreads >= 2.0.10],
    AC_SUBST(LIBEVENT_PTHREADS_CFLAGS) AC_SUBST(LIBEVENT_PTHREADS_LIBS)
)

# bash completion
PKG_CHECK_MODULES([BASH_COMPLETION], [bash-completion >= 2.0],
    [BASH_COMPLETION_DIR="`pkg-config --variable=completionsdir bash-completion`"],
    [BASH_COMPLETION_DIR="$datadir/bash-completion/completions"])
AC_SUBST([BASH_COMPLETION_DIR])

AC_CONFIG_FILES([
   	Makefile
])
AC_OUTPUT
