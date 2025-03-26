# jigsawMetaService
a distributed manager service based on etcdv3 and backend using redis cluster

sudo apt-get install -y \
    libgrpc++-dev \
    libprotobuf-dev \
    protobuf-compiler \
    libssl-dev \
    libhiredis-dev \
    libcpprest-dev \
    protobuf-compiler-grpc \
    libspdlog-dev

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

// compile
mkdir build && cd build
cmake ..
make -j4

./meta_service
