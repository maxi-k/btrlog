package org.example.bookkeeper.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Main BookKeeper benchmark application.
 * Maps to the main() function and primary() logic in the Rust benchmark.
 */
public class BookKeeperBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(BookKeeperBenchmark.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        try {
            // Check if first argument is 'open' or 'closed'
            if (args.length == 0) {
                System.err.println("Usage: java BookKeeperBenchmark <open|closed> [options...]");
                System.err.println("  open   - Run open benchmark (constant arrival rate)");
                System.err.println("  closed - Run closed benchmark (closed loop)");
                System.exit(1);
            }

            String benchmarkType = args[0];
            String[] benchArgs = Arrays.copyOfRange(args, 1, args.length);

            if ("open".equalsIgnoreCase(benchmarkType)) {
                // Run open benchmark
                OpenBenchConfig config = OpenBenchConfig.parseArgs(benchArgs);
                setLogLevel(config.logLevel);
                logger.info("Starting BookKeeper open benchmark with config: {}", config);
                OpenBenchmark benchmark = new OpenBenchmark(config);
                benchmark.run();

            } else if ("closed".equalsIgnoreCase(benchmarkType)) {
                // Run closed benchmark (original)
                BenchConfig config = BenchConfig.parseArgs(benchArgs);
                setLogLevel(config.logLevel);
                logger.info("Starting BookKeeper closed benchmark with config: {}", config);
                runBenchmark(config);

            } else {
                System.err.println("Unknown benchmark type: " + benchmarkType);
                System.err.println("Use 'open' or 'closed'");
                System.exit(1);
            }

        } catch (Exception e) {
            logger.error("Benchmark failed", e);
            System.exit(1);
        }
    }

    /**
     * Run the main benchmark.
     * Maps to the primary() function in the Rust benchmark.
     */
    private static void runBenchmark(BenchConfig config) throws Exception {
        logger.info("Starting benchmark with {} threads, {} tasks per thread, {} total operations",
                   config.threadCount, config.tasksPerThread, config.opCount);

        // Shared operation counter across all threads (equivalent to Arc<AtomicUsize>)
        AtomicLong opCounter = new AtomicLong(0);
        
        // Progress monitoring task
        ScheduledExecutorService progressMonitor = startProgressMonitor(opCounter, config);
        
        // Thread pool for worker threads
        ExecutorService threadPool = Executors.newFixedThreadPool(config.threadCount,
            new ThreadFactory() {
                private int threadCounter = 0;
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "BenchWorker-" + threadCounter++);
                    t.setDaemon(false);
                    return t;
                }
            });

        try {
            Instant startTime = Instant.now();
            
            // Spawn worker threads (equivalent to spawn_n_scoped_executors)
            List<Future<ClientStats>> threadFutures = new ArrayList<>();
            for (int threadId = 0; threadId < config.threadCount; threadId++) {
                WorkerThread worker = new WorkerThread(config, opCounter, threadId);
                threadFutures.add(threadPool.submit(worker));
            }

            logger.info("Spawned {} worker threads, waiting for completion", config.threadCount);

            // Wait for all threads to complete and collect statistics
            List<ClientStats> threadStats = new ArrayList<>();
            for (int i = 0; i < threadFutures.size(); i++) {
                try {
                    ClientStats stats = threadFutures.get(i).get();
                    threadStats.add(stats);
                    logger.info("Worker thread {} completed with {} requests", i, stats.getRequestCount());
                } catch (ExecutionException e) {
                    logger.error("Worker thread {} failed", i, e.getCause());
                    throw new RuntimeException("Worker thread execution failed", e.getCause());
                }
            }

            Instant endTime = Instant.now();
            long durationMs = endTime.toEpochMilli() - startTime.toEpochMilli();
            
            // Merge statistics from all threads
            ClientStats globalStats = ClientStats.merge(threadStats);
            
            logger.info("Benchmark completed in {}ms with {} total operations",
                       durationMs, globalStats.getRequestCount());
            
            // Output results (same format as Rust benchmark)
            outputResults(config, globalStats, durationMs);
            
        } finally {
            // Clean up
            progressMonitor.shutdown();
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
     * Start a background task to monitor benchmark progress.
     * Maps to the progress monitoring task in the Rust benchmark.
     */
    private static ScheduledExecutorService startProgressMonitor(AtomicLong opCounter, BenchConfig config) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ProgressMonitor");
            t.setDaemon(true);
            return t;
        });
        
        scheduler.scheduleAtFixedRate(() -> {
            long currentOps = opCounter.get();
            if (currentOps >= config.opCount) {
                logger.info("Benchmark complete with {} operations", currentOps);
            } else {
                double percentComplete = (double) currentOps / config.opCount * 100.0;
                logger.info("Benchmark progress: {}/{} operations ({:.1f}%)", 
                           currentOps, config.opCount, percentComplete);
            }
        }, 1, 1, TimeUnit.SECONDS);
        
        return scheduler;
    }

    /**
     * Output benchmark results in the same format as the Rust benchmark.
     * Maps to output_benchmark_result() and CSV output in the Rust benchmark.
     */
    private static void outputResults(BenchConfig config, ClientStats globalStats, long durationMs) {
        try {
            // Collect environment metadata
            BenchmarkEnvironment environment = BenchmarkEnvironment.collect(config);
            
            // Create comprehensive benchmark result
            Map<String, Object> result = new HashMap<>();
            result.put("config", configToMap(config));
            result.put("environment", environment);
            result.put("statistics", globalStats.toJson());
            result.put("duration_ms", durationMs);
            
            // Output JSON (same format as Rust benchmark)
            String jsonResult = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(result);
            
            System.out.println("\n=== BENCHMARK RESULT JSON ===");
            System.out.println(jsonResult);
            System.out.println("=== END BENCHMARK RESULT ===\n");
            
            // Output CSV for quick analysis (same format as Rust benchmark)
            outputCsvSummary(globalStats, durationMs);
            
        } catch (Exception e) {
            logger.error("Failed to output benchmark results", e);
        }
    }
    
    /**
     * Convert BenchConfig to a Map for JSON serialization.
     */
    private static Map<String, Object> configToMap(BenchConfig config) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("op_count", config.opCount);
        configMap.put("ledger_creation_probability", config.ledgerCreationProbability);
        configMap.put("max_ledger_count", config.maxLedgerCount);
        configMap.put("initial_ledger_count", config.initialLedgerCount);
        configMap.put("thread_count", config.threadCount);
        configMap.put("tasks_per_thread", config.tasksPerThread);
        configMap.put("min_message_size", config.minMessageSize);
        configMap.put("max_message_size", config.maxMessageSize);
        configMap.put("ensemble_size", config.ensembleSize);
        configMap.put("write_quorum_size", config.writeQuorumSize);
        configMap.put("ack_quorum_size", config.ackQuorumSize);
        configMap.put("track_percentiles", config.trackPercentiles);
        configMap.put("track_ledger_latency", config.trackLedgerLatency);
        return configMap;
    }
    
    /**
     * Output CSV summary for quick analysis.
     */
    private static void outputCsvSummary(ClientStats stats, long durationMs) {
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
        
        // Additional CSV columns (matches Rust benchmark format)
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
     * Set the log level programmatically for the benchmark.
     */
    private static void setLogLevel(String logLevel) {
        try {
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            Configuration config = context.getConfiguration();
            
            org.apache.logging.log4j.Level level = org.apache.logging.log4j.Level.toLevel(logLevel.toUpperCase());
            
            // Set the root logger level
            LoggerConfig rootConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
            rootConfig.setLevel(level);
            
            // Set our benchmark package logger level (add if not exists)
            LoggerConfig benchmarkConfig = config.getLoggerConfig("org.example.bookkeeper.benchmark");
            benchmarkConfig.setLevel(level);
            
            // Update loggers
            context.updateLoggers();
            
            logger.info("Log level set to: {}", level);
        } catch (Exception e) {
            logger.warn("Failed to set log level to '{}', using default: {}", logLevel, e.getMessage());
        }
    }
}