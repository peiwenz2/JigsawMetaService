# intance-1 ~ instance-99
./redis_data_prepare.sh -s 1 -e 100 int instance

# 200000 keys, prefix: block_prefix_. value is string length 256
./redis_data_prepare.sh -s 1 -e 200000 string block_prefix_
