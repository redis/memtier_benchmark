FROM ubuntu:22.04 as builder
RUN apt-get update
RUN \
  DEBIAN_FRONTEND=noninteractive \
  apt-get install -y \
    build-essential autoconf automake libevent-dev \
    pkg-config zlib1g-dev libssl-dev libboost-all-dev cmake flex
COPY . /memtier_benchmark
WORKDIR /memtier_benchmark
RUN autoreconf -ivf && ./configure && make && make install

FROM ubuntu:22.04
LABEL Description="memtier_benchmark"
COPY --from=builder /usr/local/bin/memtier_benchmark /usr/local/bin/memtier_benchmark
RUN \
  apt-get update && \
  DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends \
      libevent-dev \
  && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/

ENTRYPOINT ["memtier_benchmark"]
