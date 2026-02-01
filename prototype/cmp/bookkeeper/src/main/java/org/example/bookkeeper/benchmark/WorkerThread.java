package org.example.bookkeeper.benchmark;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Worker thread that manages BookKeeper client and spawns benchmark tasks.
 * Maps to the benchmark_thread() function in the Rust benchmark.
 */
public class WorkerThread implements Callable<ClientStats> {
    private static final Logger logger = LoggerFactory.getLogger(WorkerThread.class);
    
    private final BenchConfig config;
    private final AtomicLong opCounter;
    private final int threadId;

    public WorkerThread(BenchConfig config, AtomicLong opCounter, int threadId) {
        this.config = config;
        this.opCounter = opCounter;
        this.threadId = threadId;
    }

    @Override
    public ClientStats call() throws Exception {
        logger.info("Worker thread {} starting with {} tasks", threadId, config.tasksPerThread);
        
        // Initialize BookKeeper client for this thread
        BookKeeper bookKeeper = null;
        ExecutorService taskPool = null;
        
        try {
            // Create BookKeeper client configuration
            ClientConfiguration clientConf = createClientConfiguration();
            
            // Initialize BookKeeper client
            logger.debug("Thread {} connecting to BookKeeper at {}", threadId, config.zkServers);
            bookKeeper = new BookKeeper(clientConf);
            
            // Create executor for tasks within this thread
            taskPool = Executors.newFixedThreadPool(config.tasksPerThread, 
                new ThreadFactory() {
                    private int taskCounter = 0;
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, String.format("BenchTask-%d-%d", threadId, taskCounter++));
                        t.setDaemon(true);
                        return t;
                    }
                });
            
            // Spawn benchmark tasks (equivalent to async task spawning in Rust)
            List<Future<ClientStats>> taskFutures = new ArrayList<>();
            for (int taskId = 0; taskId < config.tasksPerThread; taskId++) {
                BenchmarkTask task = new BenchmarkTask(bookKeeper, config, opCounter, threadId, taskId);
                taskFutures.add(taskPool.submit(task));
            }
            
            logger.info("Thread {} spawned {} tasks, waiting for completion", threadId, taskFutures.size());
            
            // Wait for all tasks to complete and collect their statistics
            List<ClientStats> taskStats = new ArrayList<>();
            for (Future<ClientStats> future : taskFutures) {
                try {
                    ClientStats stats = future.get();
                    taskStats.add(stats);
                    logger.debug("Thread {} task completed with {} requests", 
                               threadId, stats.getRequestCount());
                } catch (ExecutionException e) {
                    logger.error("Thread {} task failed", threadId, e.getCause());
                    throw new RuntimeException("Task execution failed", e.getCause());
                }
            }
            
            // Merge statistics from all tasks in this thread
            ClientStats mergedStats = ClientStats.merge(taskStats);
            
            logger.info("Thread {} completed with {} total requests, avg latency: {:.2f}us", 
                       threadId, mergedStats.getRequestCount(), mergedStats.getAvgMajorityLatencyUs());
            
            return mergedStats;
            
        } finally {
            // Clean up resources
            if (taskPool != null) {
                taskPool.shutdown();
                try {
                    if (!taskPool.awaitTermination(30, TimeUnit.SECONDS)) {
                        logger.warn("Thread {} task pool did not terminate within 30 seconds", threadId);
                        taskPool.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    taskPool.shutdownNow();
                }
            }
            
            if (bookKeeper != null) {
                try {
                    bookKeeper.close();
                    logger.debug("Thread {} BookKeeper client closed", threadId);
                } catch (Exception e) {
                    logger.warn("Thread {} error closing BookKeeper client: {}", threadId, e.getMessage());
                }
            }
        }
    }

    /**
     * Create BookKeeper client configuration.
     * Maps to the client configuration in the Rust benchmark.
     */
    private ClientConfiguration createClientConfiguration() {
        ClientConfiguration conf = new ClientConfiguration();
        
        // ZooKeeper settings
        conf.setZkServers(config.zkServers);
        conf.setZkTimeout(config.zkTimeout);
        
        // Performance settings (match journal-service optimizations)
        conf.setClientTcpNoDelay(true);
        conf.setClientConnectTimeoutMillis(config.addEntryTimeoutMs);
        conf.setAddEntryTimeout(config.addEntryTimeoutMs);
        conf.setReadEntryTimeout(config.addEntryTimeoutMs);
        
        // Retry settings
        conf.setNumChannelsPerBookie(1);  // Keep it simple for benchmarking
        conf.setUseV2WireProtocol(true);  // Use latest protocol
        
        // Digest settings
        conf.setBookieRecoveryDigestType(BookKeeper.DigestType.CRC32);
        
        // Logging
        conf.setClientWriteBufferLowWaterMark(0);
        conf.setClientWriteBufferHighWaterMark(64 * 1024);
        
        // For benchmarking, we want aggressive timeouts to match journal-service behavior
        conf.setEnsemblePlacementPolicy(org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy.class);
        
        logger.debug("Created client configuration: zkServers={}, addEntryTimeout={}ms", 
                    config.zkServers, config.addEntryTimeoutMs);
        
        return conf;
    }
}