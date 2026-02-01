package org.example.bookkeeper.benchmark;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Callable;

/**
 * Core benchmark task that executes operations against BookKeeper.
 * Maps to the execute_batch() function and task logic in the Rust benchmark.
 */
public class BenchmarkTask implements Callable<ClientStats> {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkTask.class);
    private static final int BATCH_SIZE = 1000;
    
    private final BookKeeper bookKeeper;
    private final BenchConfig config;
    private final AtomicLong opCounter;
    private final int threadId;
    private final int taskId;
    private final ClientStats stats;
    private final List<LedgerClientState> ledgers;
    private final ThreadLocalRandom random;
    
    // Precomputed threshold for ledger creation probability
    private final long createThreshold;
    private final int maxLedgersPerTask;

    public BenchmarkTask(BookKeeper bookKeeper, BenchConfig config, AtomicLong opCounter, 
                        int threadId, int taskId) {
        this.bookKeeper = bookKeeper;
        this.config = config;
        this.opCounter = opCounter;
        this.threadId = threadId;
        this.taskId = taskId;
        this.stats = new ClientStats(config.trackPercentiles);
        this.ledgers = new ArrayList<>();
        this.random = ThreadLocalRandom.current();
        
        // Convert probability to threshold (like Rust benchmark)
        if (config.ledgerCreationProbability <= 0.0) {
            this.createThreshold = 0;
        } else if (config.ledgerCreationProbability >= 1.0) {
            this.createThreshold = Long.MAX_VALUE;
        } else {
            this.createThreshold = (long)(config.ledgerCreationProbability * Long.MAX_VALUE);
        }
        
        // Calculate max ledgers per task to avoid exceeding global limit
        this.maxLedgersPerTask = config.maxLedgerCount / (config.threadCount * config.tasksPerThread);
        
        logger.debug("Task {}-{} initialized with createThreshold={}, maxLedgers={}", 
                    threadId, taskId, createThreshold, maxLedgersPerTask);
    }

    @Override
    public ClientStats call() throws Exception {
        try {
            // Pre-create initial ledgers (equivalent to initial journal creation)
            createInitialLedgers();
            
            logger.info("Task {}-{} created {} initial ledgers, starting benchmark loop", 
                       threadId, taskId, ledgers.size());
            
            // Batch operation loop (matches Rust benchmark pattern)
            while (true) {
                long startIdx = opCounter.getAndAdd(BATCH_SIZE);
                if (startIdx >= config.opCount) {
                    break;
                }
                
                long endIdx = Math.min(startIdx + BATCH_SIZE, config.opCount);
                logger.trace("Task {}-{} executing batch {}-{}", threadId, taskId, startIdx, endIdx);
                
                executeBatch(startIdx, endIdx);
            }
            
            logger.info("Task {}-{} completed with {} operations", threadId, taskId, stats.getRequestCount());
            return stats;
            
        } finally {
            // Clean up ledger handles
            closeLedgers();
        }
    }

    /**
     * Create initial ledgers for this task.
     * Maps to the initial journal creation loop in the Rust benchmark.
     */
    private void createInitialLedgers() throws Exception {
        for (int i = 0; i < config.initialLedgerCount; i++) {
            createLedger();
        }
    }

    /**
     * Execute a batch of operations, mixing ledger creation and entry appends.
     * Maps to the execute_batch() function in the Rust benchmark.
     */
    private void executeBatch(long startIdx, long endIdx) throws Exception {
        for (long opIdx = startIdx; opIdx < endIdx; opIdx++) {
            // Decide whether to create a new ledger (same logic as Rust benchmark)
            boolean shouldCreate = random.nextLong() <= createThreshold && 
                                 ledgers.size() < maxLedgersPerTask;
            
            if (shouldCreate) {
                createLedger();
            } else {
                appendEntry();
            }
        }
    }

    /**
     * Create a new ledger.
     * Maps to the journal creation logic in the Rust benchmark.
     */
    private void createLedger() throws Exception {
        if (ledgers.size() >= maxLedgersPerTask) {
            logger.trace("Task {}-{} skipping ledger creation (at max {})", 
                        threadId, taskId, maxLedgersPerTask);
            return;
        }
        
        long startTime = System.nanoTime();
        int retries = 0;
        
        try {
            // Create ledger with quorum settings
            // writeQuorumSize = all nodes (like journal creation requiring all nodes)
            // ackQuorumSize = majority (for consistency)
            LedgerHandle ledger = bookKeeper.createLedger(
                config.ensembleSize,
                config.writeQuorumSize,  // All nodes for creates
                config.ackQuorumSize,    // Majority for acks
                BookKeeper.DigestType.CRC32,
                "benchmark".getBytes()
            );
            
            long latency = System.nanoTime() - startTime;
            
            if (config.trackLedgerLatency) {
                stats.recordLedgerCreateLatency(latency);
            }
            
            ledgers.add(new LedgerClientState(ledger));
            
            logger.trace("Task {}-{} created ledger {} in {}us", 
                        threadId, taskId, ledger.getId(), latency / 1000);
                        
        } catch (Exception e) {
            retries++;
            logger.warn("Task {}-{} failed to create ledger (retry {}): {}", 
                       threadId, taskId, retries, e.getMessage());
            
            // For now, just rethrow. Could add retry logic here if needed.
            throw e;
        }
    }

    /**
     * Append an entry to an existing ledger.
     * Maps to the entry append logic in the Rust benchmark.
     */
    private void appendEntry() throws Exception {
        if (ledgers.isEmpty()) {
            logger.trace("Task {}-{} skipping append (no ledgers)", threadId, taskId);
            return;
        }
        
        // Random ledger selection (same as Rust benchmark)
        LedgerClientState state = ledgers.get(random.nextInt(ledgers.size()));
        
        // Random payload size (same distribution as Rust benchmark)
        int size = random.nextInt(config.maxMessageSize - config.minMessageSize) 
                  + config.minMessageSize;
        byte[] payload = state.makeRequest(size);
        
        long sendStartTime = System.nanoTime();
        long majorityStartTime = sendStartTime;  // For BookKeeper, these are the same
        int retries = 0;
        
        try {
            // Add entry (BookKeeper handles quorum internally)
            long entryId = state.addEntry(payload);
            
            long endTime = System.nanoTime();
            long sendLatency = endTime - sendStartTime;
            long majorityLatency = endTime - majorityStartTime;
            
            // Record statistics (same format as Rust benchmark)
            stats.recordAppendLatency(majorityLatency, sendLatency, payload.length, retries);
            
            logger.trace("Task {}-{} appended entry {} to ledger {} in {}us", 
                        threadId, taskId, entryId, state.getLedgerId(), majorityLatency / 1000);
                        
        } catch (Exception e) {
            retries++;
            logger.warn("Task {}-{} failed to append to ledger {} (retry {}): {}", 
                       threadId, taskId, state.getLedgerId(), retries, e.getMessage());
            
            // For now, just rethrow. Could add retry logic here if needed.
            throw e;
        }
    }

    /**
     * Close all ledger handles when the task is done.
     */
    private void closeLedgers() {
        for (LedgerClientState state : ledgers) {
            try {
                long startTime = System.nanoTime();
                state.close();
                long closeLatencyNs = System.nanoTime() - startTime;
                stats.recordLedgerCloseLatency(closeLatencyNs);
                logger.trace("Closed ledger {} in {}us", state.getLedgerId(), closeLatencyNs / 1000);
            } catch (Exception e) {
                logger.warn("Error closing ledger {}: {}", state.getLedgerId(), e.getMessage());
            }
        }
        ledgers.clear();
    }
}