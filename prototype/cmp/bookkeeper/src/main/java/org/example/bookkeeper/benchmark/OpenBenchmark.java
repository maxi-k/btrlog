package org.example.bookkeeper.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Open benchmark implementation for BookKeeper.
 * Maps to the primary() function in open_bench.rs.
 */
public class OpenBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(OpenBenchmark.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int MORSEL_SIZE = 100;

    private final OpenBenchConfig config;
    private final AtomicLong globalOpCounter;
    private final AtomicInteger ledgersCreated;
    private final AtomicInteger nextLedgerCreator;
    private final Instant startupTimestamp;
    private ClientStats globalStats;
    private final BlockingQueue<ClientStats> statsQueue;

    public OpenBenchmark(OpenBenchConfig config) {
        this.config = config;
        this.globalOpCounter = new AtomicLong(0);
        this.ledgersCreated = new AtomicInteger(0);
        this.nextLedgerCreator = new AtomicInteger(0);
        this.startupTimestamp = Instant.now();
        this.globalStats = new ClientStats(config.trackPercentiles);
        this.statsQueue = new LinkedBlockingQueue<>();
    }

    /**
     * Run the open benchmark.
     */
    public void run() throws Exception {
        logger.info("Starting open benchmark with config: {}", config);

        // Start stats collection task
        ScheduledExecutorService statsCollector = startStatsCollector();

        // Create thread pool for worker threads
        ExecutorService threadPool = Executors.newFixedThreadPool(config.primaryThreadCount,
            new ThreadFactory() {
                private int threadCounter = 0;
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "OpenBenchWorker-" + threadCounter++);
                    t.setDaemon(false);
                    return t;
                }
            });

        try {
            Instant benchStartTime = Instant.now();

            // Spawn worker threads
            List<Future<Void>> threadFutures = new ArrayList<>();
            for (int threadId = 0; threadId < config.primaryThreadCount; threadId++) {
                OpenBenchWorker worker = new OpenBenchWorker(threadId);
                threadFutures.add(threadPool.submit(worker));
            }

            logger.info("Spawned {} worker threads, waiting for completion", config.primaryThreadCount);

            // Wait for all threads to complete
            for (int i = 0; i < threadFutures.size(); i++) {
                try {
                    threadFutures.get(i).get();
                    logger.info("Worker thread {} completed", i);
                } catch (ExecutionException e) {
                    logger.error("Worker thread {} failed", i, e.getCause());
                    throw new RuntimeException("Worker thread execution failed", e.getCause());
                }
            }

            Instant benchEndTime = Instant.now();
            long durationMs = benchEndTime.toEpochMilli() - benchStartTime.toEpochMilli();

            // Collect any remaining stats
            ClientStats finalStats = new ClientStats(config.trackPercentiles);
            List<ClientStats> statsList = new ArrayList<>();
            statsQueue.drainTo(statsList);
            statsList.add(globalStats);
            finalStats = ClientStats.merge(statsList);

            logger.info("Benchmark completed in {}ms with {} total operations",
                durationMs, finalStats.getRequestCount());

            // Output results
            outputResults(finalStats, durationMs);

        } finally {
            // Clean up
            statsCollector.shutdown();
            threadPool.shutdown();

            try {
                if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                    logger.warn("Thread pool did not terminate within 60 seconds");
                    threadPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                threadPool.shutdownNow();
            }
        }
    }

    /**
     * Worker thread that manages ledger tasks.
     * Maps to the scoped executor spawned in primary().
     */
    private class OpenBenchWorker implements Callable<Void> {
        private final int threadId;
        private final AtomicLong localOps;
        private final AtomicBoolean keepRunning;
        private BookKeeper bookKeeper;

        public OpenBenchWorker(int threadId) {
            this.threadId = threadId;
            this.localOps = new AtomicLong(0);
            this.keepRunning = new AtomicBoolean(true);
        }

        @Override
        public Void call() throws Exception {
            try {
                // Initialize BookKeeper client for this thread
                ClientConfiguration clientConf = createClientConfiguration();
                bookKeeper = new BookKeeper(clientConf);

                logger.info("Thread {} connected to BookKeeper", threadId);

                // Thread pool for ledger tasks
                ExecutorService ledgerTaskPool = Executors.newCachedThreadPool(
                    new ThreadFactory() {
                        private int taskCounter = 0;
                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(r, String.format("LedgerTask-%d-%d", threadId, taskCounter++));
                            t.setDaemon(false);
                            return t;
                        }
                    });

                try {
                    List<Future<?>> taskFutures = new ArrayList<>();

                    // Create initial ledger tasks
                    for (int i = 0; i < config.initialLogCount; i++) {
                        taskFutures.add(ledgerTaskPool.submit(createLedgerTask()));
                    }

                    // Ledger creation loop
                    while (keepRunning.get()) {
                        // Check if we should create a new ledger
                        long sleepMs = shouldCreateLedger();
                        if (sleepMs == 0) {
                            // Create new ledger task
                            taskFutures.add(ledgerTaskPool.submit(createLedgerTask()));
                            ledgersCreated.incrementAndGet();
                        } else {
                            // Sleep and retry
                            Thread.sleep(sleepMs);
                        }
                    }

                    // Wait for all ledger tasks to complete
                    for (Future<?> future : taskFutures) {
                        try {
                            future.get();
                        } catch (ExecutionException e) {
                            logger.error("Ledger task failed", e.getCause());
                        }
                    }

                } finally {
                    ledgerTaskPool.shutdown();
                    try {
                        if (!ledgerTaskPool.awaitTermination(30, TimeUnit.SECONDS)) {
                            logger.warn("Thread {} ledger task pool did not terminate within 30 seconds", threadId);
                            ledgerTaskPool.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        ledgerTaskPool.shutdownNow();
                    }
                }

                logger.info("Thread {} completed", threadId);
                return null;

            } finally {
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
         * Create a ledger task.
         */
        private LedgerTask createLedgerTask() {
            return new LedgerTask(
                bookKeeper,
                config,
                keepRunning,
                this::acquireOpTicket,
                this::recordStats
            );
        }

        /**
         * Check if we should create a new ledger.
         * Maps to should_create_journal() in open_bench.rs.
         * @return 0 if we should create, sleep duration in ms otherwise
         */
        private long shouldCreateLedger() {
            // Check if it's this thread's turn
            if (threadId != nextLedgerCreator.get()) {
                return config.logCreationIntervalMs;
            }

            Instant now = Instant.now();
            long timeSinceStartMs = now.toEpochMilli() - startupTimestamp.toEpochMilli();
            double ledgersShouldBe = (double) timeSinceStartMs / config.logCreationIntervalMs;
            double ledgersActual = ledgersCreated.get();

            if (ledgersShouldBe > ledgersActual) {
                // Time to create a new ledger
                nextLedgerCreator.set((threadId + 1) % config.primaryThreadCount);
                return 0;
            } else {
                // Calculate sleep time until next ledger should be created
                double fraction = 1.0 - (ledgersShouldBe - Math.floor(ledgersShouldBe));
                return Math.max(1, (long) (fraction * config.logCreationIntervalMs));
            }
        }

        /**
         * Try to acquire an operation ticket.
         * Maps to acquire_op_ticket() in open_bench.rs.
         */
        private boolean acquireOpTicket() {
            long opsLeft = localOps.get();
            if (opsLeft > 0) {
                localOps.set(opsLeft - 1);
                return true;
            }

            long globalCursor = globalOpCounter.getAndAdd(MORSEL_SIZE);
            if (globalCursor >= config.opCount) {
                keepRunning.set(false);
                return false;
            }

            long opsToTake = Math.min(MORSEL_SIZE, config.opCount - globalCursor);
            if (opsToTake > 0) {
                localOps.set(opsToTake - 1);
                return true;
            } else {
                keepRunning.set(false);
                return false;
            }
        }

        /**
         * Record statistics from a ledger task.
         */
        private void recordStats(ClientStats stats) {
            try {
                statsQueue.put(stats);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while recording stats");
            }
        }

        /**
         * Create BookKeeper client configuration.
         */
        private ClientConfiguration createClientConfiguration() {
            ClientConfiguration conf = new ClientConfiguration();

            // ZooKeeper settings
            conf.setZkServers(config.zkServers);
            conf.setZkTimeout(config.zkTimeout);

            // Performance settings
            conf.setClientTcpNoDelay(true);
            conf.setClientConnectTimeoutMillis(config.addEntryTimeoutMs);
            conf.setAddEntryTimeout(config.addEntryTimeoutMs);
            conf.setReadEntryTimeout(config.addEntryTimeoutMs);

            // Retry settings
            conf.setNumChannelsPerBookie(1);
            conf.setUseV2WireProtocol(true);

            // Digest settings
            conf.setBookieRecoveryDigestType(BookKeeper.DigestType.CRC32);

            // Buffer settings
            conf.setClientWriteBufferLowWaterMark(0);
            conf.setClientWriteBufferHighWaterMark(64 * 1024);

            // Placement policy
            conf.setEnsemblePlacementPolicy(org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy.class);

            logger.debug("Created client configuration: zkServers={}, addEntryTimeout={}ms",
                config.zkServers, config.addEntryTimeoutMs);

            return conf;
        }
    }

    /**
     * Start background stats collection task.
     * Maps to the stats collection task in primary().
     */
    private ScheduledExecutorService startStatsCollector() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "StatsCollector");
            t.setDaemon(true);
            return t;
        });

        Instant globalStartTime = Instant.now();
        AtomicLong lastScheduleTime = new AtomicLong(globalStartTime.toEpochMilli());
        AtomicInteger lastLedgerCount = new AtomicInteger(0);
        AtomicBoolean headerPrinted = new AtomicBoolean(false);

        scheduler.scheduleAtFixedRate(() -> {
            try {
                long currentOps = globalOpCounter.get();

                // Collect stats from queue
                ClientStats currentStats = new ClientStats(config.trackPercentiles);
                List<ClientStats> statsList = new ArrayList<>();
                statsList.add(currentStats);
                statsQueue.drainTo(statsList);
                currentStats = ClientStats.merge(statsList);

                // Merge into global stats
                synchronized (globalStats) {
                    globalStats = ClientStats.merge(Arrays.asList(globalStats, currentStats));
                }

                // Print progress as CSV
                long now = Instant.now().toEpochMilli();
                long timestampMs = now - globalStartTime.toEpochMilli();
                long durationMs = now - lastScheduleTime.getAndSet(now);
                int currentLedgerCount = ledgersCreated.get();
                int journalsCreated = currentLedgerCount - lastLedgerCount.getAndSet(currentLedgerCount);
                
                // Print CSV header once
                if (!headerPrinted.getAndSet(true)) {
                    System.out.println("timestamp_ms,duration_ms,req_cnt,maj_lat_avg_us,req_size_avg,journals_created,maj_lat_p50_us,maj_lat_p95_us,maj_lat_p99_us,maj_lat_p99_9_us");
                }
                
                long requestCount = currentStats.getRequestCount();
                double majLatAvg = requestCount > 0 ? currentStats.getAvgMajorityLatencyUs() : 0.0;
                double reqSizeAvg = requestCount > 0 ? currentStats.getAvgRequestSize() : 0.0;
                
                String csvLine;
                if (currentStats.tracksPercentiles() && requestCount > 0) {
                    csvLine = String.format("%d,%d,%d,%.2f,%.2f,%d,%d,%d,%d,%d",
                        timestampMs, durationMs, requestCount, majLatAvg, reqSizeAvg, journalsCreated,
                        currentStats.calculatePercentile(50.0),
                        currentStats.calculatePercentile(95.0),
                        currentStats.calculatePercentile(99.0),
                        currentStats.calculatePercentile(99.9));
                } else {
                    csvLine = String.format("%d,%d,%d,%.2f,%.2f,%d,0,0,0,0",
                        timestampMs, durationMs, requestCount, majLatAvg, reqSizeAvg, journalsCreated);
                }
                
                System.out.println(csvLine);
                
                if (currentOps >= config.opCount) {
                    logger.info("Benchmark complete with {} operations", currentOps);
                }
            } catch (Exception e) {
                logger.error("Error in stats collector", e);
            }
        }, config.printIntervalMs, config.printIntervalMs, TimeUnit.MILLISECONDS);

        return scheduler;
    }

    /**
     * Output benchmark results.
     */
    private void outputResults(ClientStats stats, long durationMs) {
        try {
            // Collect environment metadata
            BenchmarkEnvironment environment = collect(config);

            // Create comprehensive benchmark result
            Map<String, Object> result = new HashMap<>();
            result.put("system", "bookkeeper");
            result.put("config", configToMap(config));
            result.put("environment", environment);
            result.put("statistics", stats.toJson());
            result.put("duration_ms", durationMs);

            // Output JSON
            String jsonResult = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(result);

            System.out.println("\n=== BENCHMARK RESULT JSON ===");
            System.out.println(jsonResult);
            System.out.println("=== END BENCHMARK RESULT ===\n");

            // Output CSV summary
            outputCsvSummary(stats, durationMs);

        } catch (Exception e) {
            logger.error("Failed to output benchmark results", e);
        }
    }

    /**
     * Convert config to map for JSON serialization.
     */
    private Map<String, Object> configToMap(OpenBenchConfig config) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("op_count", config.opCount);
        configMap.put("log_creation_interval_ms", config.logCreationIntervalMs);
        configMap.put("min_log_push_interval_ms", config.minLogPushIntervalMs);
        configMap.put("max_log_push_interval_ms", config.maxLogPushIntervalMs);
        configMap.put("print_interval_ms", config.printIntervalMs);
        configMap.put("primary_thread_count", config.primaryThreadCount);
        configMap.put("initial_log_count", config.initialLogCount);
        configMap.put("min_message_size", config.minMessageSize);
        configMap.put("max_message_size", config.maxMessageSize);
        configMap.put("ensemble_size", config.ensembleSize);
        configMap.put("write_quorum_size", config.writeQuorumSize);
        configMap.put("ack_quorum_size", config.ackQuorumSize);
        configMap.put("track_percentiles", config.trackPercentiles);
        configMap.put("track_journal_latency", config.trackJournalLatency);
        return configMap;
    }

    /**
     * Output CSV summary.
     */
    private void outputCsvSummary(ClientStats stats, long durationMs) {
        System.out.println("\n=== CSV SUMMARY ===");
        System.out.println("metric,value");
        System.out.println("duration_ms," + durationMs);
        System.out.println("requests," + stats.getRequestCount());
        System.out.println("throughput_ops_per_sec," + (stats.getRequestCount() * 1000.0 / durationMs));
        System.out.println("avg_latency_us," + String.format("%.2f", stats.getAvgMajorityLatencyUs()));
        System.out.println("avg_request_size_bytes," + String.format("%.2f", stats.getAvgRequestSize()));
        System.out.println("avg_retries," + String.format("%.2f", stats.getAvgRetries()));

        // Percentile metrics (if enabled)
        if (stats.tracksPercentiles()) {
            System.out.println("latency_p50_us," + stats.calculatePercentile(50.0));
            System.out.println("latency_p90_us," + stats.calculatePercentile(90.0));
            System.out.println("latency_p95_us," + stats.calculatePercentile(95.0));
            System.out.println("latency_p99_us," + stats.calculatePercentile(99.0));
            System.out.println("latency_p999_us," + stats.calculatePercentile(99.9));
        }

        // Detailed CSV
        String[] csvColumns = stats.csvColumns();
        System.out.println("\n=== DETAILED CSV ===");
        String csvHeader = "request_count,maj_lat_min_us,maj_lat_max_us,maj_lat_avg_us," +
                          "send_lat_min_us,send_lat_max_us,send_lat_avg_us," +
                          "req_size_min,req_size_max,req_size_avg," +
                          "retries_min,retries_max,retries_avg";
        if (stats.tracksPercentiles()) {
            csvHeader += ",maj_lat_p50_us,maj_lat_p90_us,maj_lat_p95_us,maj_lat_p99_us,maj_lat_p999_us";
        }
        System.out.println(csvHeader);
        System.out.println(String.join(",", csvColumns));
        System.out.println("=== END CSV ===\n");
    }

    /**
     * Helper method to convert config to BenchmarkEnvironment-compatible object.
     */
    private static BenchmarkEnvironment collect(OpenBenchConfig config) {
        // Create a temporary closed BenchConfig for environment collection
        // This is a bit hacky but reuses the existing BenchmarkEnvironment code
        BenchConfig tempConfig = new BenchConfig.Builder()
            .opCount(config.opCount)
            .threadCount(config.primaryThreadCount)
            .tasksPerThread(1)
            .minMessageSize(config.minMessageSize)
            .maxMessageSize(config.maxMessageSize)
            .ensembleSize(config.ensembleSize)
            .writeQuorumSize(config.writeQuorumSize)
            .ackQuorumSize(config.ackQuorumSize)
            .zkServers(config.zkServers)
            .build();
        return BenchmarkEnvironment.collect(tempConfig);
    }
}
