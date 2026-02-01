package org.example.bookkeeper.benchmark;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Task that manages a single ledger with constant arrival rate.
 * Maps to spawn_journal_task() in open_bench.rs.
 */
public class LedgerTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(LedgerTask.class);

    private final BookKeeper bookKeeper;
    private final OpenBenchConfig config;
    private final AtomicBoolean keepRunning;
    private final Supplier<Boolean> onAcquireOp;
    private final Consumer<ClientStats> onRecordStats;
    private final Random random;
    private final ZipfDistribution sizeDistribution;

    public LedgerTask(
            BookKeeper bookKeeper,
            OpenBenchConfig config,
            AtomicBoolean keepRunning,
            Supplier<Boolean> onAcquireOp,
            Consumer<ClientStats> onRecordStats) {
        this.bookKeeper = bookKeeper;
        this.config = config;
        this.keepRunning = keepRunning;
        this.onAcquireOp = onAcquireOp;
        this.onRecordStats = onRecordStats;
        this.random = ThreadLocalRandom.current();

        // Zipf distribution samples from 1 to n, so we adjust for the min/max range
        double sizeRange = config.maxMessageSize - config.minMessageSize + 1;
        this.sizeDistribution = new ZipfDistribution(sizeRange, config.zipfFactor, this.random);
    }

    @Override
    public void run() {
        LedgerHandle ledger = null;
        long timeOverhangNanos = 0;

        try {
            // Create the ledger for this task
            ledger = createLedger();
            if (ledger == null) {
                logger.error("Failed to create ledger, task exiting");
                return;
            }

            if (config.trackJournalLatency) {
                ClientStats stats = new ClientStats(false);
                stats.recordLedgerCreateLatency(0); // Latency already recorded in createLedger
                onRecordStats.accept(stats);
            }

            LedgerClientState state = new LedgerClientState(ledger);

            // Main push loop with constant arrival rate
            while (keepRunning.get()) {
                // Try to acquire an operation ticket
                boolean acquired = acquireOpTicket();
                if (!acquired) {
                    break;
                }

                long startTime = System.nanoTime();

                // Push to ledger
                pushToLedger(state);

                long endTime = System.nanoTime();
                long opLatency = endTime - startTime;

                // Calculate sleep time to maintain constant arrival rate
                // Random interval between min and max push interval
                long pushIntervalMs = config.minLogPushIntervalMs +
                    random.nextInt((int)(config.maxLogPushIntervalMs - config.minLogPushIntervalMs + 1));
                long pushIntervalNanos = pushIntervalMs * 1_000_000 + timeOverhangNanos;

                if (pushIntervalNanos <= opLatency) {
                    // Operation took longer than desired interval, no sleep needed
                    timeOverhangNanos = 0;
                    continue;
                }

                // Sleep to maintain arrival rate
                // Use millisecond resolution sleep, track overhang in nanos
                long sleepNanos = pushIntervalNanos - opLatency;
                long sleepMs = sleepNanos / 1_000_000;
                long remainderNanos = sleepNanos % 1_000_000;

                // Carry over sub-millisecond remainder to next iteration
                timeOverhangNanos = remainderNanos;

                if (sleepMs > 0) {
                    try {
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.warn("Ledger task interrupted during sleep");
                        break;
                    }
                }
            }

        } catch (Exception e) {
            logger.error("Ledger task failed", e);
        } finally {
            // Close the ledger
            if (ledger != null) {
                try {
                    long startTime = System.nanoTime();
                    ledger.close();
                    long closeLatency = System.nanoTime() - startTime;

                    ClientStats stats = new ClientStats(false);
                    stats.recordLedgerCloseLatency(closeLatency);
                    onRecordStats.accept(stats);

                    logger.debug("Closed ledger {} in {}us", ledger.getId(), closeLatency / 1000);
                } catch (Exception e) {
                    logger.warn("Error closing ledger: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * Create a ledger for this task.
     * Maps to create_journal() in open_bench.rs.
     */
    private LedgerHandle createLedger() {
        try {
            long startTime = System.nanoTime();

            LedgerHandle ledger = bookKeeper.createLedger(
                config.ensembleSize,
                config.writeQuorumSize,
                config.ackQuorumSize,
                BookKeeper.DigestType.CRC32,
                "benchmark".getBytes()
            );

            long createLatency = System.nanoTime() - startTime;

            if (config.trackJournalLatency) {
                ClientStats stats = new ClientStats(false);
                stats.recordLedgerCreateLatency(createLatency);
                onRecordStats.accept(stats);
            }

            logger.debug("Created ledger {} in {}us", ledger.getId(), createLatency / 1000);
            return ledger;

        } catch (Exception e) {
            logger.error("Failed to create ledger", e);
            return null;
        }
    }

    /**
     * Push an entry to the ledger.
     * Maps to push_to_journal() in open_bench.rs.
     */
    private void pushToLedger(LedgerClientState state) {
        try {
            // Generate payload size from Zipfian distribution
            int size = sizeDistribution.sample() + config.minMessageSize - 1;
            byte[] payload = state.makeRequest(size);

            // Measure latency
            long sendStartTime = System.nanoTime();
            long entryId = state.addEntry(payload);
            long endTime = System.nanoTime();

            long latency = endTime - sendStartTime;

            // Record statistics
            ClientStats stats = new ClientStats(config.trackPercentiles);
            stats.recordAppendLatency(latency, latency, payload.length, 0);
            onRecordStats.accept(stats);

            logger.trace("Appended entry {} to ledger {} in {}us",
                entryId, state.getLedgerId(), latency / 1000);

        } catch (Exception e) {
            logger.warn("Failed to append entry to ledger {}: {}",
                state.getLedgerId(), e.getMessage());
        }
    }

    /**
     * Try to acquire an operation ticket from the global pool.
     * Maps to acquire_op_ticket() in open_bench.rs.
     */
    private boolean acquireOpTicket() {
        return onAcquireOp.get();
    }
}
