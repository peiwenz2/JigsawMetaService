package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"
    "sort"
	"sync"
	"fmt"
	"unsafe"

	"github.com/redis/go-redis/v9"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	clusterClient *redis.ClusterClient
	logger        *log.Logger
    cfg           *Config
)

type Config struct {
	RedisCluster RedisClusterConfig `json:"redis_cluster"`
}

type RedisClusterConfig struct {
	Hosts        []string         `json:"hosts"`
	Config       ClusterOptions   `json:"config"`
	MaxBatchSize int              `json:"max_batch_size"`
    MaxConcurrentBatches int      `json:"max_concurrent_routines"`
    QueryTimeout time.Duration    `json:"query_timeout_second"`
	LogConfig    LogConfig        `json:"log_config"`
}

type ClusterOptions struct {
	ConnectTimeoutMs int `json:"connect_timeout_ms"`
	SocketTimeoutMs  int `json:"socket_timeout_ms"`
	PoolSize         int `json:"pool_size"`
}

type LogConfig struct {
	FilePath    string `json:"file_path"`
	MaxSizeMB   int    `json:"max_size_mb"`
	MaxBackups  int    `json:"max_backups"`
}

type ZMember struct {
    Member string  `json:"member"`
    Score  float64 `json:"score"`
}

//export Initialize
func Initialize(configPath *C.char) *C.char {
	cfgFile := C.GoString(configPath)
	configData, err := os.ReadFile(cfgFile)
	if err != nil {
		return C.CString(fmt.Sprintf("read json file failed: %v", err))
	}

	// decode
	var tempConfig Config
	if err := json.Unmarshal(configData, &tempConfig); err != nil {
		return C.CString(fmt.Sprintf("decode json file failed: %v", err))
	}
	cfg = &tempConfig

	// init log
	initLogger()

	if err := connectRedisCluster(); err != nil {
        logger.Printf("RedisCluster Initialize error: %v.", err.Error())
		return C.CString(err.Error())
	}
    logger.Printf("RedisCluster Initialize.")

	return C.CString("")
}

func initLogger() {
	logCfg := cfg.RedisCluster.LogConfig

	if logCfg.FilePath == "" {
		logCfg.FilePath = "/var/log/redis_wrapper.log"
	}
	if logCfg.MaxSizeMB == 0 {
		logCfg.MaxSizeMB = 10
	}
	if logCfg.MaxBackups == 0 {
		logCfg.MaxBackups = 10
	}

	logOutput := &lumberjack.Logger{
		Filename:   logCfg.FilePath,
		MaxSize:    logCfg.MaxSizeMB,
		MaxBackups: logCfg.MaxBackups,
	}
	logger = log.New(logOutput, "[RedisGo] ", log.LstdFlags|log.Lshortfile)
    logger.Printf("Logger init.")
}

func connectRedisCluster() error {
	redisCfg := cfg.RedisCluster
	opts := &redis.ClusterOptions{
		Addrs:        redisCfg.Hosts,
		DialTimeout:  time.Duration(redisCfg.Config.ConnectTimeoutMs) * time.Millisecond,
		ReadTimeout:  time.Duration(redisCfg.Config.SocketTimeoutMs) * time.Millisecond,
		WriteTimeout: time.Duration(redisCfg.Config.SocketTimeoutMs) * time.Millisecond,
		PoolSize:     redisCfg.Config.PoolSize,
	}

	clusterClient = redis.NewClusterClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(redisCfg.Config.ConnectTimeoutMs)*time.Millisecond)
	defer cancel()
	return clusterClient.Ping(ctx).Err()
}

//export SingleRead
func SingleRead(key *C.char) *C.char {
    ctx, cancel := context.WithTimeout(context.Background(), cfg.RedisCluster.QueryTimeout * time.Second)
    defer cancel()

    goKey := C.GoString(key)
    val, err := clusterClient.Get(ctx, goKey).Result()
    if err == redis.Nil {
        return C.CString("")
    }
    if err != nil {
        logger.Printf("SingleRead failed for key %s: %v", goKey, err)
        return nil
    }
    //logger.Printf("SingleRead ok for key %s: %v", goKey, val)
    return C.CString(val)
}

//export SingleWrite
func SingleWrite(key, value *C.char, with_set_name *C.char) C.int {
    ctx, cancel := context.WithTimeout(context.Background(), cfg.RedisCluster.QueryTimeout * time.Second)
    defer cancel()

    goKey := C.GoString(key)
    goValue := C.GoString(value)
    setName := C.GoString(with_set_name)
    err := clusterClient.Set(ctx, goKey, goValue, 0).Err()
    if err != nil {
        logger.Printf("SingleWrite failed for key %s: %v", goKey, err)
        return C.int(1)
    }
    if setName != "" {
        err = clusterClient.SAdd(ctx, setName, goKey).Err()
        if err != nil {
            logger.Printf("SADD failed for set %s (key %s): %v", setName, goKey, err)
        }
    }
    return C.int(0)
}

//export SingleZRead
func SingleZRead(zsetKey *C.char, member *C.char) *C.char {
    ctx, cancel := context.WithTimeout(context.Background(), cfg.RedisCluster.QueryTimeout * time.Second)
    defer cancel()

    goZSetKey := C.GoString(zsetKey)
    goMember := C.GoString(member)

    score, err := clusterClient.ZScore(ctx, goZSetKey, goMember).Result()
    if err == redis.Nil {
        return C.CString("")  // member not exist
    }
    if err != nil {
        logger.Printf("SingleZRead failed for zset %s member %s: %v", goZSetKey, goMember, err)
        return nil
    }

    return C.CString(fmt.Sprintf("%f", score))
}

//export SingleZWrite
func SingleZWrite(zsetKey *C.char, member *C.char, score C.double, withSetName *C.char) C.int {
    ctx, cancel := context.WithTimeout(context.Background(), cfg.RedisCluster.QueryTimeout * time.Second)
    defer cancel()

    goZSetKey := C.GoString(zsetKey)
    goMember := C.GoString(member)
    goScore := float64(score)
    setName := C.GoString(withSetName)

    err := clusterClient.ZAdd(ctx, goZSetKey, redis.Z{
        Score:  goScore,
        Member: goMember,
    }).Err()
    if err != nil {
        logger.Printf("SingleZWrite failed for zset %s member %s: %v", goZSetKey, goMember, err)
        return C.int(1)
    }

    if setName != "" {
        err = clusterClient.SAdd(ctx, setName, goZSetKey).Err()
        if err != nil {
            logger.Printf("SADD failed for set %s (zset key %s): %v", setName, goZSetKey, err)
        }
    }
    //logger.Printf("SingleZWrite ok for zset %s member %s", goZSetKey, goMember)

    return C.int(0)
}

//export SingleZReadRange
func SingleZReadRange(zsetKey *C.char, isTopN C.int, n C.int) *C.char {
    ctx, cancel := context.WithTimeout(context.Background(), cfg.RedisCluster.QueryTimeout*time.Second)
    defer cancel()

    goZSetKey := C.GoString(zsetKey)
    var redisMembers []redis.Z

    if isTopN == 1 {
        redisMembers, _ = clusterClient.ZRevRangeWithScores(ctx, goZSetKey, 0, int64(n-1)).Result()
    } else {
        redisMembers, _ = clusterClient.ZRangeWithScores(ctx, goZSetKey, 0, -1).Result()
    }

    var jsonMembers []ZMember
    for _, z := range redisMembers {
        member, ok := z.Member.(string)
        if !ok {
            continue
        }
        jsonMembers = append(jsonMembers, ZMember{
            Member: member,
            Score:  z.Score,
        })
    }

    jsonData, _ := json.Marshal(jsonMembers)
    return C.CString(string(jsonData))
}

//export SingleZRem
func SingleZRem(zsetKey *C.char, member *C.char) C.int {
    ctx, cancel := context.WithTimeout(context.Background(), cfg.RedisCluster.QueryTimeout * time.Second)
    defer cancel()

    goZSetKey := C.GoString(zsetKey)
    goMember := C.GoString(member)

    // remove a specific member from zset
    _, err := clusterClient.ZRem(ctx, goZSetKey, goMember).Result()
    if err != nil {
        logger.Printf("ZRem failed for %s member %s: %v", goZSetKey, goMember, err)
        return C.int(1)
    }
    logger.Printf("ZRem ok for %s member %s", goZSetKey, goMember)
    return C.int(0)
}

//export FreeCString
func FreeCString(s *C.char) {
    C.free(unsafe.Pointer(s))
}

func batchWrite(keys, values []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.RedisCluster.QueryTimeout * time.Second)
	defer cancel()

	pipe := clusterClient.Pipeline()
	for i := range keys {
		pipe.Set(ctx, keys[i], values[i], 0)
	}

	_, err := pipe.Exec(ctx)
	return err
}

//export SafeBatchWrite
func SafeBatchWrite(keys, values **C.char, count C.int) *C.char {
    startTime := time.Now()
    logger.Printf("Starting SafeBatchWrite with %d items", count)

    cKeys := unsafe.Slice(keys, int(count))
    cVals := unsafe.Slice(values, int(count))

    goKeys := make([]string, count)
    goVals := make([]string, count)
    for i := range cKeys {
        goKeys[i] = C.GoString(cKeys[i])
        goVals[i] = C.GoString(cVals[i])
    }

    var (
        wg          sync.WaitGroup
        errChan     = make(chan error, 1)
        ctx, cancel = context.WithCancel(context.Background())
    )
    defer cancel()

    batches := splitIntoKeyValueBatches(goKeys, goVals, cfg.RedisCluster.MaxBatchSize)

    sem := make(chan struct{}, cfg.RedisCluster.MaxConcurrentBatches)
    for i, batch := range batches {
        select {
        case <-ctx.Done():
            break
        default:
            wg.Add(1)
            sem <- struct{}{}

            go func(batchIndex int, keys, values []string) {
                defer func() {
                    <-sem
                    wg.Done()
                }()

                if err := batchWrite(keys, values); err != nil {
                    select {
                    case errChan <- fmt.Errorf("batch %d write failed: %w", batchIndex, err):
                        cancel()
                    default:
                    }
                }
            }(i, batch.keys, batch.values)
        }
    }

    go func() {
        wg.Wait()
        close(errChan)
    }()

    if err := <-errChan; err != nil {
        logger.Printf("SafeBatchWrite failed: %v", err)
        return C.CString(err.Error())
    }

    logger.Printf("SafeBatchWrite completed in %v", time.Since(startTime))
    return C.CString("")
}

func splitIntoKeyValueBatches(keys, values []string, batchSize int) []struct {
    keys   []string
    values []string
} {
    var batches []struct {
        keys   []string
        values []string
    }
    for i := 0; i < len(keys); i += batchSize {
        end := i + batchSize
        if end > len(keys) {
            end = len(keys)
        }
        batches = append(batches, struct {
            keys   []string
            values []string
        }{
            keys:   keys[i:end],
            values: values[i:end],
        })
    }
    return batches
}

func batchRead(keys []string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.RedisCluster.QueryTimeout * time.Second)
	defer cancel()

	pipe := clusterClient.Pipeline()
	cmds := make([]*redis.StringCmd, len(keys))

	for i, key := range keys {
		cmds[i] = pipe.Get(ctx, key)
	}

	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	results := make([]string, len(keys))
	for i, cmd := range cmds {
		val, err := cmd.Result()
		if err == redis.Nil {
			results[i] = ""
		} else if err != nil {
			return nil, err
		} else {
			results[i] = val
		}
	}
	return results, nil
}

//export SafeBatchRead
func SafeBatchRead(keys **C.char, count C.int, results ***C.char) *C.char {
    startTime := time.Now()
    logger.Printf("Starting SafeBatchRead with %d items", count)

    cKeys := unsafe.Slice(keys, int(count))
    goKeys := make([]string, int(count))
    for i := range cKeys {
        goKeys[i] = C.GoString(cKeys[i])
    }

    allResults := make([]string, len(goKeys))

    var (
        wg        sync.WaitGroup
        errChan   = make(chan error, 1)
        ctx, cancel = context.WithCancel(context.Background())
    )
    defer cancel()

    type batch struct {
        startIdx int
        keys     []string
    }
    var batches []batch
    for i := 0; i < len(goKeys); i += cfg.RedisCluster.MaxBatchSize {
        end := i + cfg.RedisCluster.MaxBatchSize
        if end > len(goKeys) {
            end = len(goKeys)
        }
        batches = append(batches, batch{
            startIdx: i,
            keys:     goKeys[i:end],
        })
    }

    sem := make(chan struct{}, cfg.RedisCluster.MaxConcurrentBatches)
    for _, b := range batches {
        select {
        case <-ctx.Done():
            break
        default:
            sem <- struct{}{}
            wg.Add(1)

            go func(b batch) {
                defer func() {
                    <-sem
                    wg.Done()
                }()

                batchResults, err := batchRead(b.keys)
                if err != nil {
                    select {
                    case errChan <- fmt.Errorf("batch failed at %d-%d: %w", 
                        b.startIdx, b.startIdx+len(b.keys)-1, err):
                        cancel()
                    default:
                    }
                    return
                }

                copy(allResults[b.startIdx:], batchResults)
            }(b)
        }
    }

    go func() {
        wg.Wait()
        close(errChan)
    }()

    if err := <-errChan; err != nil {
        logger.Printf("BatchRead failed: %v", err)
        return C.CString(err.Error())
    }

    resultSlice := make([]*C.char, len(allResults))
    for i, res := range allResults {
        resultSlice[i] = C.CString(res)
    }
    *results = (**C.char)(unsafe.Pointer(&resultSlice[0]))

    logger.Printf("SafeBatchRead completed in %v", time.Since(startTime))
    return C.CString("")
}

func splitIntoBatches(items []string, batchSize int) [][]string {
    var batches [][]string
    for i := 0; i < len(items); i += batchSize {
        end := i + batchSize
        if end > len(items) {
            end = len(items)
        }
        batches = append(batches, items[i:end])
    }
    return batches
}

//export SafeBatchZWrite
func SafeBatchZWrite(
    cKeys **C.char,
    cMembers **C.char,
    cScores *C.double,
    count C.int,
) *C.char {
    startTime := time.Now()
    defer func() {
        logger.Printf("SafeBatchZWrite completed in %v", time.Since(startTime))
    }()

    goKeys := unsafe.Slice(cKeys, int(count))
    goMembers := unsafe.Slice(cMembers, int(count))
    goScores := unsafe.Slice(cScores, int(count))

    entries := make([]struct {
        key    string
        member string
        score  float64
    }, count)

    for i := 0; i < int(count); i++ {
        entries[i] = struct {
            key    string
            member string
            score  float64
        }{
            key:    C.GoString(goKeys[i]),
            member: C.GoString(goMembers[i]),
            score:  float64(goScores[i]),
        }
    }

    var (
        wg      sync.WaitGroup
        errChan = make(chan error, 1)
        ctx, cancel = context.WithCancel(context.Background())
    )
    defer cancel()

    sem := make(chan struct{}, cfg.RedisCluster.MaxConcurrentBatches)
    batches := chunkSlice(entries, cfg.RedisCluster.MaxBatchSize)

    for i, batch := range batches {
        select {
        case <-ctx.Done():
            break
        default:
            wg.Add(1)
            sem <- struct{}{}

            go func(batchIndex int, batch []struct{key, member string; score float64}) {
                defer func() {
                    <-sem
                    wg.Done()
                }()

                pipe := clusterClient.Pipeline()
                for _, entry := range batch {
                    pipe.ZAdd(ctx, entry.key, redis.Z{
                        Score:  entry.score,
                        Member: entry.member,
                    })
                }

                if _, err := pipe.Exec(ctx); err != nil {
                    select {
                    case errChan <- fmt.Errorf("batch %d failed: %w", batchIndex, err):
                        cancel()
                    default:
                    }
                }
            }(i, batch)
        }
    }

    go func() {
        wg.Wait()
        close(errChan)
    }()

    if err := <-errChan; err != nil {
        logger.Printf("SafeBatchZWrite failed: %v", err)
        return C.CString(err.Error())
    }
    return nil
}

func chunkSlice(slice []struct{key, member string; score float64}, size int) [][]struct{key, member string; score float64} {
    var chunks [][]struct{key, member string; score float64}
    for i := 0; i < len(slice); i += size {
        end := i + size
        if end > len(slice) {
            end = len(slice)
        }
        chunks = append(chunks, slice[i:end])
    }
    return chunks
}

//export SafeBatchZRead
func SafeBatchZRead(requests **C.char, count C.int) *C.char {
    type ReadRequest struct {
        ZsetKey string
        TopN    int
    }

    cRequests := unsafe.Slice(requests, int(count))
    reqs := make([]ReadRequest, count)
    for i := range reqs {
        reqs[i] = ReadRequest{
            ZsetKey: C.GoString(cRequests[i]),
            TopN:    0,
        }
    }

    result := make(map[string][]ZMember)
    var mu sync.Mutex
    var wg sync.WaitGroup
    sem := make(chan struct{}, cfg.RedisCluster.MaxConcurrentBatches)

    for _, req := range reqs {
        wg.Add(1)
        sem <- struct{}{}

        go func(r ReadRequest) {
            defer func() {
                <-sem
                wg.Done()
            }()

            members := readZSet(r.ZsetKey, r.TopN)
            mu.Lock()
            result[r.ZsetKey] = members
            mu.Unlock()
        }(req)
    }

    wg.Wait()

    jsonData, _ := json.Marshal(result)
    return C.CString(string(jsonData))
}

func readZSet(zsetKey string, topN int) []ZMember {
    ctx, cancel := context.WithTimeout(context.Background(), cfg.RedisCluster.QueryTimeout*time.Second)
    defer cancel()

    var redisMembers []redis.Z
    if topN > 0 {
        redisMembers, _ = clusterClient.ZRevRangeWithScores(ctx, zsetKey, 0, int64(topN-1)).Result()
    } else {
        redisMembers, _ = clusterClient.ZRangeWithScores(ctx, zsetKey, 0, -1).Result()
    }

    var jsonMembers []ZMember
    for _, z := range redisMembers {
        if member, ok := z.Member.(string); ok {
            jsonMembers = append(jsonMembers, ZMember{
                Member: member,
                Score:  z.Score,
            })
        }
    }
    return jsonMembers
}

//export RemoveKeyFromSet
func RemoveKeyFromSet(setName, key *C.char) C.int {
    ctx, cancel := context.WithTimeout(context.Background(), cfg.RedisCluster.QueryTimeout * time.Second)
    defer cancel()

    goSetName := C.GoString(setName)
    goKey := C.GoString(key)

    // Skip if set name is empty
    if goSetName == "" {
        return C.int(0)
    }

    // Remove the key from the set
    _, err := clusterClient.SRem(ctx, goSetName, goKey).Result()
    if err != nil {
        logger.Printf("SREM failed for set %s (key %s): %v", goSetName, goKey, err)
        return C.int(1)
    }

    return C.int(0)
}

//export GetKeysInSet
func GetKeysInSet(setName *C.char, keyCount *C.int) **C.char {
    ctx, cancel := context.WithTimeout(context.Background(), cfg.RedisCluster.QueryTimeout * time.Second)
    defer cancel()

    goSetName := C.GoString(setName)

    // 1. Get all members from the Redis Set
    keys, err := clusterClient.SMembers(ctx, goSetName).Result()
    if err != nil {
        logger.Printf("SMEMBERS failed for set %s: %v", goSetName, err)
        return nil
    }

    // 2. Create C array of char* (size +1 for NULL terminator)
    cArray := C.malloc(C.size_t(len(keys)+1) * C.size_t(unsafe.Sizeof(uintptr(0))))
    cStrings := (*[1<<30 - 1]*C.char)(cArray)
    *keyCount = C.int(len(keys))

    // 3. Convert Go strings to C strings
    for i, key := range keys {
        cStrings[i] = C.CString(key)
    }

    // 4. Add NULL terminator at the end
    cStrings[len(keys)] = nil

    return (**C.char)(cArray)
}

//export ScanInstanceKeys
func ScanInstanceKeys(matchPattern *C.char, batchSize C.int, keyCount *C.int) **C.char {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    pattern := C.GoString(matchPattern)
    batch := int(batchSize)
    logger.Printf("ScanInstanceKeys start: pattern=%s batch=%d", pattern, batch)

    masterClients, err := getClusterMasters(ctx)
    if err != nil {
        logger.Printf("Failed to get cluster masters: %v", err)
        return nil
    }

    var (
        allKeys   []string
        keysMutex sync.Mutex
        errs      []error
        errMutex  sync.Mutex
        wg        sync.WaitGroup
    )

    limiter := make(chan struct{}, 48) // Concurrency limiter

    for _, client := range masterClients {
        limiter <- struct{}{} // Block if buffer is full to limit concurrency
        wg.Add(1)
        go func(c *redis.Client) {
            defer func() {
                <-limiter // Release slot
                wg.Done()
            }()

            if err := scanSingleNode(ctx, c, pattern, batch, func(keys []string) {
                keysMutex.Lock()
                allKeys = append(allKeys, keys...)
                keysMutex.Unlock()
            }); err != nil {
                errMutex.Lock()
                errs = append(errs, fmt.Errorf("node %s: %v", c.Options().Addr, err))
                errMutex.Unlock()
                logger.Printf("Error scanning node %s: %v", c.Options().Addr, err)
            }
        }(client)
    }

    // Wait for all workers to finish or context timeout
    done := make(chan struct{})
    go func() {
        wg.Wait()
        close(done)
    }()

    select {
    case <-done:
        // All workers completed
    case <-ctx.Done():
        logger.Printf("ScanInstanceKeys timeout: %v", ctx.Err())
        return nil
    }

    // Check for any errors
    errMutex.Lock()
    scanErrors := errs
    errMutex.Unlock()

    if len(scanErrors) > 0 {
        logger.Printf("ScanInstanceKeys failed with errors: %v", scanErrors)
        return nil
    }

    keysMutex.Lock()
    defer keysMutex.Unlock()

    count := len(allKeys)
    *keyCount = C.int(count)  // Set output parameter

    if count == 0 {
        return nil
    }

    // Allocate C memory for array (no NULL terminator)
    cArray := (**C.char)(C.malloc(C.size_t(count) * C.size_t(unsafe.Sizeof(uintptr(0)))))
    pointers := (*[1<<30 - 1]*C.char)(unsafe.Pointer(cArray))

    // Convert strings and populate array
    for i, key := range allKeys {
        pointers[i] = C.CString(key)
    }

    return cArray
}

func getClusterMasters(ctx context.Context) ([]*redis.Client, error) {
    var masters []*redis.Client
    err := clusterClient.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
        masters = append(masters, client)
        return nil
    })
    return masters, err
}

// scan by each node
/*           Nodes
 *    |        |        |
 *   node    node     node
 *  | | |   | | |     | | |
 * mutiple goroutines to speed up
 */
func scanSingleNode(
    ctx context.Context,
    client *redis.Client,
    pattern string,
    batchSize int,
    collectFunc func([]string),
) error {
    var (
        cursor   uint64
        scanned  int
        maxRetry = 3
    )

    for {
        select {
        case <-ctx.Done():
            logger.Printf("Abort scanning %s: %v", client.Options().Addr, ctx.Err())
            return nil
        default:
            keys, nextCursor, err := retryScan(ctx, client, cursor, pattern, batchSize, maxRetry)
            if err != nil {
                return err
            }

            scanned += len(keys)
            collectFunc(keys)
            // logger.Printf("scanSingleNode Node %s: scanned %d keys (cursor=%d)", client.Options().Addr, len(keys), cursor)

            // stop
            if nextCursor == 0 {
                logger.Printf("Node %s scan completed, total=%d", client.Options().Addr, scanned)
                return nil
            }
            cursor = nextCursor
        }
    }
}

// scan with retry
func retryScan(
    ctx context.Context,
    client *redis.Client,
    cursor uint64,
    pattern string,
    count, maxRetry int,
) ([]string, uint64, error) {
    var err error
    for i := 0; i < maxRetry; i++ {
        keys, nextCursor, e := client.Scan(ctx, cursor, pattern, int64(count)).Result()
        if e == nil {
            // logger.Printf("retryScan ok, keys len:%v", len(keys))
            return keys, nextCursor, nil
        }
        err = e
        logger.Printf("Scan failed on %s (retry %d/%d): %v", client.Options().Addr, i+1, maxRetry, err)
        time.Sleep(100 * time.Millisecond) //TODO(zhangpeiwen.zhangp) 指数退避更好，此处简化
    }
    return nil, 0, fmt.Errorf("scan failed after %d retries: %w", maxRetry, err)
}

func batchDelete(keys []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.RedisCluster.QueryTimeout*time.Second)
	defer cancel()

	pipe := clusterClient.Pipeline()
	for _, key := range keys {
		pipe.Del(ctx, key)
	}

	_, err := pipe.Exec(ctx)
	return err
}

//export SafeBatchDelete
func SafeBatchDelete(keys **C.char, count C.int) *C.char {
	startTime := time.Now()
	logger.Printf("Starting SafeBatchDelete with %d items", count)

	cKeys := unsafe.Slice(keys, int(count))
	goKeys := make([]string, int(count))
	for i := range cKeys {
		goKeys[i] = C.GoString(cKeys[i])
	}

	var (
		wg          sync.WaitGroup
		errChan     = make(chan error, 1)
		ctx, cancel = context.WithCancel(context.Background())
	)
	defer cancel()

	batches := splitIntoBatches(goKeys, cfg.RedisCluster.MaxBatchSize)

	sem := make(chan struct{}, cfg.RedisCluster.MaxConcurrentBatches)
	for i, batch := range batches {
		select {
		case <-ctx.Done():
			break
		default:
			wg.Add(1)
			sem <- struct{}{}

			go func(batchIndex int, keys []string) {
				defer func() {
					<-sem
					wg.Done()
				}()

				if err := batchDelete(keys); err != nil {
					select {
					case errChan <- fmt.Errorf("batch %d delete failed: %w", batchIndex, err):
						cancel()
					default:
					}
				}
			}(i, batch)
		}
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	if err := <-errChan; err != nil {
		logger.Printf("SafeBatchDelete failed: %v", err)
		return C.CString(err.Error())
	}

	logger.Printf("SafeBatchDelete completed in %v", time.Since(startTime))
	return C.CString("")
}

//export GetHottestKeys
func GetHottestKeys(prefix *C.char, batchSize C.int, topN C.int, keyCount *C.int) **C.char {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    keysPtr := ScanInstanceKeys(prefix, batchSize, keyCount)
    if keysPtr == nil || *keyCount == 0 {
        return nil
    }

    count := int(*keyCount)
    cKeys := (*[1<<30 -1]*C.char)(unsafe.Pointer(keysPtr))[:count:count]
    allKeys := make([]string, count)
    for i := 0; i < count; i++ {
        allKeys[i] = C.GoString(cKeys[i])
    }

    //logger.Printf("ScanInstanceKeys with prefix %v, keys: %v", prefix, allKeys)

    type keyHeat struct {
        key  string
        heat int
    }

    var (
        heatChan   = make(chan keyHeat, count)
        errChan    = make(chan error, count)
        wg         sync.WaitGroup
        limiter    = make(chan struct{}, 48) // 并发控制
    )

    for _, key := range allKeys {
        wg.Add(1)
        limiter <- struct{}{}
        go func(k string) {
            defer func() {
                <-limiter
                wg.Done()
            }()

            val, err := clusterClient.Do(ctx, "OBJECT", "FREQ", k).Int()
            if err != nil {
                errChan <- fmt.Errorf("key %s: %v", k, err)
                return
            }
            logger.Printf("Key %v FREQ %v ", k, val)
            heatChan <- keyHeat{k, val}
        }(key)
    }

    go func() {
        wg.Wait()
        close(heatChan)
        close(errChan)
    }()

    heats := make([]keyHeat, 0, count)
    for kh := range heatChan {
        heats = append(heats, kh)
    }

    if len(errChan) > 0 {
        logger.Printf("GetHottestKeys partial errors (%d): %v", len(errChan), <-errChan)
    }

    sort.Slice(heats, func(i, j int) bool {
        return heats[i].heat > heats[j].heat
    })

    if len(heats) > int(topN) {
        heats = heats[:topN]
    }

    *keyCount = C.int(len(heats))
    if len(heats) == 0 {
        return nil
    }

    cArray := (**C.char)(C.malloc(C.size_t(len(heats)) * C.size_t(unsafe.Sizeof(uintptr(0)))))
    pointers := (*[1<<30 -1]*C.char)(unsafe.Pointer(cArray))

    for i, kh := range heats {
        pointers[i] = C.CString(kh.key)
    }

    //FreeScanResults(keysPtr, C.int(count))

    return cArray
}

//export FreeStrings
func FreeStrings(arr **C.char, count C.int) {
	cArray := unsafe.Slice(arr, int(count))
	for i := range cArray {
		C.free(unsafe.Pointer(cArray[i]))
	}
}

//export FreeKeyArray
func FreeKeyArray(arr **C.char, count C.int) {
    cArray := unsafe.Slice(arr, int(count))
    for i := range cArray {
        C.free(unsafe.Pointer(cArray[i]))
    }
}

//export FreeScanResults
func FreeScanResults(cArray **C.char, count C.int) {
    if cArray == nil {
        return
    }

    // Convert to slice of C strings
    pointers := (*[1<<30 - 1]*C.char)(unsafe.Pointer(cArray))[:count:count]

    // Free individual strings
    for _, s := range pointers {
        C.free(unsafe.Pointer(s))
    }

    // Free the array itself
    C.free(unsafe.Pointer(cArray))
}

func main() {
	// Ensure log directory exists
	if err := os.MkdirAll("/opt/meta_service", 0755); err != nil {
		log.Fatalf("Failed to create log directory: %v", err)
	}
}