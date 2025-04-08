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
		return C.CString(err.Error())
	}

	return C.CString("")
}

func initLogger() {
	logCfg := cfg.RedisCluster.LogConfig

	// 设置默认值
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