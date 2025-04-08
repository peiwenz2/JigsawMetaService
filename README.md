# MetaService

A Distributed Meta Data Management Service.
## Role
There is **two** roles of MetaSercice:
- Data Backend: As the storage of meta data(instance status, prefix hash kv, etc.)
- Cluster Manager: The distributed manager(1 master N followers) could conduct periodical tasks within the cluster.

Note: Currently for simplicity, the data backend is using redis cluster, and the manager process based on ETCD for master election.

## Install

### prerequisites

```
sudo apt-get install -y \
    golang \
    libgrpc++-dev \
    libprotobuf-dev \
    protobuf-compiler \
    libssl-dev \
    libhiredis-dev \
    libcpprest-dev \
    protobuf-compiler-grpc \
    libspdlog-dev \
    nlohmann-json3-dev \
    libgtest-dev \
    build-essential

export PATH=$PATH:/usr/local/go/bin
export GOPATH=$HOME/go
export PATH=$PATH:$GOPATH/bin

// etcd support
git clone https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3.git
cd etcd-cpp-apiv3
mkdir build && cd build
cmake ..
make -j$(nproc) && make install

// v1.1.0 hiredis
wget https://github.com/redis/hiredis/archive/refs/tags/v1.1.0.tar.gz
tar -xzf v1.1.0.tar.gz
cd hiredis-1.1.0
make
sudo make install

// redis
git clone https://github.com/sewenew/redis-plus-plus.git
cd redis-plus-plus
mkdir build
cd build
cmake ..
make
make install
```

### make
```
mkdir build && cd build
cmake ..
make -j4

# debug
mkdir build_debug && cd build_debug
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j4
```

## Run
```
./meta_service --config=./config/config.json
```

## LOG
```
/opt/meta_service/meta_service.LOG
```