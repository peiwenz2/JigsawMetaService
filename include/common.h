#include <fstream>
#include <vector>
#include <string>
#include <iostream>
#include <sched.h>
#include <unistd.h> 

#define META_SERVICE_DIR    "/opt/meta_service/"
#define LOG_FILENAME        META_SERVICE_DIR "meta_service.LOG"
#define LOG_FILENAME_REDIS  META_SERVICE_DIR "redis.LOG"
#define LOG_ROTATE_SIZE     1048576 * 5  // 5MB

#define REDIS_CONNECTION_TIMEOUT_MS 1000 // 1s
#define REDIS_SOCKET_TIMEOUT_MS 500

#define LEASE_EXIST_TIME    10 // 10s
#define ETCD_LEADER_CHECK_TIME  1 //1s
#define MAIN_PERIODICAL_TASK_TIME_MS 2000

std::string VectorToStringWithComma(const std::vector<std::string>& vec);
