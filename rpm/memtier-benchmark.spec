# RPM spec file for memtier_benchmark
# Copyright (C) 2011-2026 Redis Ltd.

# Version is set by the workflow via sed before building
Name:           memtier-benchmark
Version:        0.0.0
Release:        1%{?dist}
Summary:        NoSQL Redis and Memcache traffic generation and benchmarking tool

License:        GPLv2
URL:            https://github.com/redis/memtier_benchmark
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  gcc-c++
BuildRequires:  make
BuildRequires:  autoconf
BuildRequires:  automake
BuildRequires:  libtool
BuildRequires:  libevent-devel
BuildRequires:  openssl-devel
BuildRequires:  zlib-devel
BuildRequires:  pkgconfig

%description
memtier_benchmark is a command line utility developed by Redis Labs
for load generation and benchmarking NoSQL key-value databases.

It offers the following features:
  * Support for both Redis and Memcache protocols (text and binary)
  * Multi-threaded multi-client execution
  * Multiple configuration options, including:
    - Read:Write ratio
    - Random and sequential key name pattern policies
    - Random or ranged key expiration
    - Redis cluster support
    - TLS connections support

%prep
%setup -q

%build
autoreconf -ivf
%configure
# Use distribution flags + fPIC for PIE support (required on el10+)
%make_build CXXFLAGS="%{optflags} -fPIC"

%install
%make_install

%files
%license COPYING
%doc README.md
%{_bindir}/memtier_benchmark
%{_mandir}/man1/memtier_benchmark.1*
%{_datadir}/bash-completion/completions/memtier_benchmark

%changelog
* %(date "+%a %b %d %Y") Redis Team <oss@redis.com> - %{version}-%{release}
- See https://github.com/redis/memtier_benchmark/releases

