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
AC_INIT(memtier_benchmark,2.3.1,oss@redis.com)
AC_CONFIG_SRCDIR([memtier_benchmark.cpp])
AC_CONFIG_HEADER([config.h])
AM_INIT_AUTOMAKE
AM_SILENT_RULES([yes])
AC_CONFIG_MACRO_DIR([m4])
LT_INIT

CXXFLAGS="-O2 -g -Wall $CXXFLAGS"

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

# bash completion
PKG_CHECK_MODULES([BASH_COMPLETION], [bash-completion >= 2.0],
    [BASH_COMPLETION_DIR="`pkg-config --variable=completionsdir bash-completion`"],
    [BASH_COMPLETION_DIR="$datadir/bash-completion/completions"])
AC_SUBST([BASH_COMPLETION_DIR])

AC_CONFIG_FILES([
   	Makefile
])
AC_OUTPUT
