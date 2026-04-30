memtier_benchmark
=================
![GitHub](https://img.shields.io/github/license/RedisLabs/memtier_benchmark)
[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/RedisLabs/memtier_benchmark)](https://github.com/RedisLabs/memtier_benchmark/releases)
[![codecov](https://codecov.io/gh/RedisLabs/memtier_benchmark/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisLabs/memtier_benchmark)


memtier_benchmark is a command line utility developed by [Redis](https://redis.io) (formerly Garantia Data Ltd.) for load generation and benchmarking NoSQL key-value databases. It offers the following:

* Support for both Redis and Memcache protocols (text and binary)
* Multi-threaded multi-client execution
* Multiple configuration options, including:
    * Read:Write ratio
    * Random and sequential key name pattern policies
    * Random or ranged key expiration
    * Redis cluster
    * TLS support
    * ...and much more

Read more at:

* [A High Throughput Benchmarking Tool for Redis and Memcached](https://redis.io/blog/memtier_benchmark-a-high-throughput-benchmarking-tool-for-redis-memcached)
* [Pseudo-Random Data, Gaussian Access Pattern and Range Manipulation](https://redis.io/blog/new-in-memtier_benchmark-pseudo-random-data-gaussian-access-pattern-and-range-manipulation/)

## Getting Started

### Installing on Debian and Ubuntu

Pre-compiled binaries are available for these platforms from the packages.redis.io Redis APT
repository. To configure this repository, use the following steps:

```
sudo apt install lsb-release curl gpg

curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list

sudo apt-get update
```

Once configured, to install memtier_benchmark use:

```
sudo apt-get install memtier-benchmark
```

### Installing on RHEL, Rocky Linux, and AlmaLinux

Pre-compiled binaries are available for RHEL 8/9/10 compatible distributions
from the packages.redis.io YUM repository. To configure this repository, use the following steps:

```
sudo curl -fsSL https://packages.redis.io/gpg -o /etc/pki/rpm-gpg/redis-archive-keyring.gpg

sudo tee /etc/yum.repos.d/redis.repo <<'EOF'
[redis]
name=Redis
baseurl=https://packages.redis.io/rpm/el$releasever/$basearch
enabled=1
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/redis-archive-keyring.gpg
EOF
```

Once configured, to install memtier_benchmark use:

```
sudo dnf install -y memtier-benchmark
```

### Installing on Amazon Linux 2023

Amazon Linux 2023 uses a separate repository path because `$releasever` on AL2023 is `2023`,
not an `el<n>` value:

```
sudo curl -fsSL https://packages.redis.io/gpg -o /etc/pki/rpm-gpg/redis-archive-keyring.gpg

sudo tee /etc/yum.repos.d/redis.repo <<'EOF'
[redis]
name=Redis
baseurl=https://packages.redis.io/rpm/amzn2023/$basearch
enabled=1
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/redis-archive-keyring.gpg
EOF
```

Once configured, to install memtier_benchmark use:

```
sudo dnf install -y memtier-benchmark
```

### Installing on MacOS

To install memtier_benchmark on MacOS, use Homebrew:

```
brew install memtier_benchmark
```

### Installing from source

For detailed instructions on building from source, running tests, and using sanitizers for development, see [DEVELOPMENT.md](DEVELOPMENT.md).

## Using Docker

Use available images on Docker Hub:

```
# latest stable release
$ docker run --rm redislabs/memtier_benchmark:latest --help

# master branch edge build
$ docker run --rm redislabs/memtier_benchmark:edge --help
```

Or, build locally:

```
$ docker build -t memtier_benchmark .
$ docker run --rm memtier_benchmark --help
```

### Using Docker Compose
```
$ docker-compose -f docker-compose.memcached.yml up --build
```
or
```
$ docker-compose -f docker-compose.redis.yml up --build
```

## Using memtier_benchmark

See the included manpage or run:

```
$ memtier_benchmark --help
```

for command line options.

## Crash Reporting

memtier_benchmark includes built-in crash handling that automatically generates detailed bug reports when the program crashes. If you encounter a crash, the tool will print a comprehensive report including:

* Stack traces for all threads
* System and build information
* Active client connection states
* Instructions for generating core dumps

For more information on crash handling, debugging, and how to report crashes, see the [Crash Handling and Debugging](DEVELOPMENT.md#crash-handling-and-debugging) section in DEVELOPMENT.md.

### Cluster mode

#### Connections

When using the cluster-mode option, each client opens one connection for each node.
So, when using a large number of threads and clients, the user must verify that he is not
limited by the maximum number of file descriptors.

#### Using sequential key pattern

When there is an asymmetry between the Redis nodes and user set
the --requests option, there may be gaps in the generated keys.

Also, the ratio and the key generator is per client (and not connection).
In this case, setting the ratio to 1:1 does not guarantee 100% hits because
the keys spread to different connections/nodes.



### Using rate-limiting for informed benchmarking

When you impose a rate limit on your benchmark tests, you're essentially mimicking a controlled production environment. This setup is crucial for understanding how latency behaves under certain throughput constraints. Here's why benchmarking latency in a rate-limited scenario is important:


1. **Realistic Performance Metrics**: In real-world scenarios, systems often operate under various limitations. Understanding how these limitations affect latency gives you a more accurate picture of system performance, than simply running benchmarks at full stress level.

1. **Capacity Planning**: By observing latency at different rate limits, you can better plan for scaling your infrastructure. It helps in identifying at what point increased load leads to unacceptable latency, guiding decisions about when to scale up.

1. **Quality of Service (QoS) Guarantees**: For services that require a certain level of performance guarantee, knowing the latency at specific rate limits helps in setting realistic QoS benchmarks.

1. **Identifying Bottlenecks**: Rate-limited benchmarking can help in identifying bottlenecks in your system. If latency increases disproportionately with a small increase in rate limit, it may indicate a bottleneck that needs attention.

1. **Comparative Analysis**: It enables the comparison of different solutions, configurations or hardware in terms of how they handle latency under simmilar benchmark conditions.


#### Using rate-limiting in memtier

To use this feature, add the `--rate-limiting` parameter followed by the desired RPS per connection.


```
memtier_benchmark [other options] --rate-limiting=<RPS>
```

Note: When using rate-limiting together with cluster-mode option, the rate-limit is associated to the connection for each node.


#### Rate limited example: 100% writes, 1M Keys, 60 seconds benchmark at 10K RPS

```
memtier_benchmark --ratio=1:0 --test-time=60 --rate-limiting=100 -t 2 -c 50 --key-pattern=P:P --key-maximum 1000000
```

### Full latency spectrum analysis

For distributions that are non-normal, such as the latency, many “basic rules” of normally distributed statistics are violated.  Instead of computing just the mean, which tries to express the whole distribution in a single result, we can use a sampling of the distribution at intervals -- percentiles, which tell you how many requests actually would experience that delay. 


When used for normally distributed data, the samples are usually taken at regular intervals. However, since the data does not obey to a normal distribution it would be very expensive to keep equally spaced intervals of latency records while enabling large value ranges. We can apply algorithms that can calculate a good approximation of percentiles at minimal CPU and memory cost, such as [t-digest](https://github.com/tdunning/t-digest) or [HdrHistogram](https://github.com/HdrHistogram/HdrHistogram_c). On memtier_benchmark we’ve decided to use the  HdrHistogram due to its low memory footprint, high precision, zero allocation during the benchmark and constant access time.


By default Memtier will output the 50th, 99th, and 99.9th percentiles. They are the latency thresholds at which 50%, 99%, and 99.9% of commands are faster than that particular presented value. 
To output different percentiles you should use the --print-percentiles option followed by the comma separated list of values ( example: `--print-percentiles 90,99,99.9,99.99` ).

#### Saving the full latency spectrum
To save the full latencies you should use the --hdr-file-prefix option followed by the prefix name you wish the filenames to have. 
Each distinct command will be saved into two different files - one in .txt (textual format) and another in .hgrm (HistogramLogProcessor format).
The textual format can be hard to analyze solely, but you can use an [online formatter](http://hdrhistogram.github.io/HdrHistogram/plotFiles.html) to generate visual histograms from it. The .hgrm format will be later added as input to Redislabs [mbdirector](https://github.com/redislabs/mbdirector) to enable visualization of time-domain results.

Sample Visual Feel of the full latency spectrum using an [online formatter](http://hdrhistogram.github.io/HdrHistogram/plotFiles.html):
![alt text][sample_visual_histogram]


[sample_visual_histogram]: ./docs/sample_visual_histogram.png "Sample Full Latency Spectrum Histogram"
