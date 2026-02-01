package org.example.bookkeeper.benchmark;

import org.apache.bookkeeper.client.LedgerHandle;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Manages state for a single ledger within a benchmark task.
 * Equivalent to JournalClientState in the Rust benchmark.
 */
public class LedgerClientState {
    private final long ledgerId;
    private final LedgerHandle ledgerHandle;
    private volatile long nextEntryId;
    private final ThreadLocalRandom random;
    
    public LedgerClientState(LedgerHandle ledgerHandle) {
        this.ledgerHandle = ledgerHandle;
        this.ledgerId = ledgerHandle.getId();
        this.nextEntryId = 0;
        this.random = ThreadLocalRandom.current();
    }
    
    /**
     * Generate a random payload for the next entry.
     * Equivalent to random_byte_vec() in the Rust benchmark.
     */
    public byte[] makeRequest(int size) {
        byte[] payload = new byte[size];
        random.nextBytes(payload);
        return payload;
    }
    
    /**
     * Add an entry to this ledger and return the entry ID.
     * This wraps the BookKeeper addEntry call with our state tracking.
     */
    public long addEntry(byte[] data) throws Exception {
        long entryId = ledgerHandle.addEntry(data);
        nextEntryId = entryId + 1;
        return entryId;
    }
    
    /**
     * Get the ledger ID for this state.
     */
    public long getLedgerId() {
        return ledgerId;
    }
    
    /**
     * Get the underlying ledger handle.
     */
    public LedgerHandle getLedgerHandle() {
        return ledgerHandle;
    }
    
    /**
     * Get the next expected entry ID.
     */
    public long getNextEntryId() {
        return nextEntryId;
    }
    
    /**
     * Close the ledger handle when done.
     */
    public void close() throws Exception {
        if (ledgerHandle != null) {
            ledgerHandle.close();
        }
    }
    
    @Override
    public String toString() {
        return String.format("LedgerClientState{ledgerId=%d, nextEntryId=%d}", 
                           ledgerId, nextEntryId);
    }
}