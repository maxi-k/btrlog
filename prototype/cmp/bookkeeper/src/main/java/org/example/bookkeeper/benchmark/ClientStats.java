package org.example.bookkeeper.benchmark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class ClientStats {
    
    public ClientStats() {
        this(false);
    }
    
    public ClientStats(boolean trackPercentiles) {
        this.trackPercentiles = trackPercentiles;
    }
    private final LongAdder requestCount = new LongAdder();
    private final LongAdder totalMajorityLatencyUs = new LongAdder();
    private final LongAdder totalSendLatencyUs = new LongAdder();
    private final LongAdder totalRequestSize = new LongAdder();
    private final LongAdder totalRetries = new LongAdder();
    
    // Min/max tracking (using volatile for thread safety)
    private volatile long majorityLatencyMinUs = Long.MAX_VALUE;
    private volatile long majorityLatencyMaxUs = Long.MIN_VALUE;
    private volatile long sendLatencyMinUs = Long.MAX_VALUE;
    private volatile long sendLatencyMaxUs = Long.MIN_VALUE;
    private volatile long requestSizeMin = Long.MAX_VALUE;
    private volatile long requestSizeMax = Long.MIN_VALUE;
    private volatile long retriesMin = Long.MAX_VALUE;
    private volatile long retriesMax = Long.MIN_VALUE;
    
    // Ledger creation stats (optional)
    private final LongAdder ledgerCreateCount = new LongAdder();
    private final LongAdder totalLedgerCreateLatencyUs = new LongAdder();
    private volatile long ledgerCreateLatencyMinUs = Long.MAX_VALUE;
    private volatile long ledgerCreateLatencyMaxUs = Long.MIN_VALUE;
    
    // Percentile tracking for majority latency
    private final TreeMap<Long, Long> majorityLatencyHistogram = new TreeMap<>();
    private final boolean trackPercentiles;

    public void recordAppendLatency(long majorityLatencyNs, long sendLatencyNs, int requestSize, int retries) {
        long majorityLatencyUs = majorityLatencyNs / 1000;
        long sendLatencyUs = sendLatencyNs / 1000;
        
        requestCount.increment();
        totalMajorityLatencyUs.add(majorityLatencyUs);
        totalSendLatencyUs.add(sendLatencyUs);
        totalRequestSize.add(requestSize);
        totalRetries.add(retries);
        
        // Record percentile data if enabled
        if (trackPercentiles) {
            synchronized (majorityLatencyHistogram) {
                majorityLatencyHistogram.merge(majorityLatencyUs, 1L, Long::sum);
            }
        }
        
        // Update min/max atomically
        updateMinMax(majorityLatencyUs, sendLatencyUs, requestSize, retries);
    }
    
    public void recordLedgerCreateLatency(long createLatencyNs) {
        long createLatencyUs = createLatencyNs / 1000;
        
        ledgerCreateCount.increment();
        totalLedgerCreateLatencyUs.add(createLatencyUs);
        
        // Update ledger creation min/max
        synchronized (this) {
            if (createLatencyUs < ledgerCreateLatencyMinUs) {
                ledgerCreateLatencyMinUs = createLatencyUs;
            }
            if (createLatencyUs > ledgerCreateLatencyMaxUs) {
                ledgerCreateLatencyMaxUs = createLatencyUs;
            }
        }
    }
    
    // Ledger close stats
    private final LongAdder ledgerCloseCount = new LongAdder();
    private final LongAdder totalLedgerCloseLatencyUs = new LongAdder();
    private volatile long ledgerCloseLatencyMinUs = Long.MAX_VALUE;
    private volatile long ledgerCloseLatencyMaxUs = Long.MIN_VALUE;
    
    public void recordLedgerCloseLatency(long closeLatencyNs) {
        long closeLatencyUs = closeLatencyNs / 1000;
        
        ledgerCloseCount.increment();
        totalLedgerCloseLatencyUs.add(closeLatencyUs);
        
        // Update ledger close min/max
        synchronized (this) {
            if (closeLatencyUs < ledgerCloseLatencyMinUs) {
                ledgerCloseLatencyMinUs = closeLatencyUs;
            }
            if (closeLatencyUs > ledgerCloseLatencyMaxUs) {
                ledgerCloseLatencyMaxUs = closeLatencyUs;
            }
        }
    }
    
    private synchronized void updateMinMax(long majorityLatencyUs, long sendLatencyUs, long requestSize, long retries) {
        // Majority latency
        if (majorityLatencyUs < majorityLatencyMinUs) {
            majorityLatencyMinUs = majorityLatencyUs;
        }
        if (majorityLatencyUs > majorityLatencyMaxUs) {
            majorityLatencyMaxUs = majorityLatencyUs;
        }
        
        // Send latency
        if (sendLatencyUs < sendLatencyMinUs) {
            sendLatencyMinUs = sendLatencyUs;
        }
        if (sendLatencyUs > sendLatencyMaxUs) {
            sendLatencyMaxUs = sendLatencyUs;
        }
        
        // Request size
        if (requestSize < requestSizeMin) {
            requestSizeMin = requestSize;
        }
        if (requestSize > requestSizeMax) {
            requestSizeMax = requestSize;
        }
        
        // Retries
        if (retries < retriesMin) {
            retriesMin = retries;
        }
        if (retries > retriesMax) {
            retriesMax = retries;
        }
    }

    public static ClientStats merge(List<ClientStats> statsList) {
        boolean anyTrackPercentiles = statsList.stream().anyMatch(s -> s.trackPercentiles);
        ClientStats merged = new ClientStats(anyTrackPercentiles);
        
        for (ClientStats stats : statsList) {
            merged.requestCount.add(stats.requestCount.sum());
            merged.totalMajorityLatencyUs.add(stats.totalMajorityLatencyUs.sum());
            merged.totalSendLatencyUs.add(stats.totalSendLatencyUs.sum());
            merged.totalRequestSize.add(stats.totalRequestSize.sum());
            merged.totalRetries.add(stats.totalRetries.sum());
            
            // Merge ledger creation stats
            merged.ledgerCreateCount.add(stats.ledgerCreateCount.sum());
            merged.totalLedgerCreateLatencyUs.add(stats.totalLedgerCreateLatencyUs.sum());
            
            // Merge ledger close stats
            merged.ledgerCloseCount.add(stats.ledgerCloseCount.sum());
            merged.totalLedgerCloseLatencyUs.add(stats.totalLedgerCloseLatencyUs.sum());
            
            // Merge min/max values
            synchronized (merged) {
                if (stats.majorityLatencyMinUs < merged.majorityLatencyMinUs) {
                    merged.majorityLatencyMinUs = stats.majorityLatencyMinUs;
                }
                if (stats.majorityLatencyMaxUs > merged.majorityLatencyMaxUs) {
                    merged.majorityLatencyMaxUs = stats.majorityLatencyMaxUs;
                }
                
                if (stats.sendLatencyMinUs < merged.sendLatencyMinUs) {
                    merged.sendLatencyMinUs = stats.sendLatencyMinUs;
                }
                if (stats.sendLatencyMaxUs > merged.sendLatencyMaxUs) {
                    merged.sendLatencyMaxUs = stats.sendLatencyMaxUs;
                }
                
                if (stats.requestSizeMin < merged.requestSizeMin) {
                    merged.requestSizeMin = stats.requestSizeMin;
                }
                if (stats.requestSizeMax > merged.requestSizeMax) {
                    merged.requestSizeMax = stats.requestSizeMax;
                }
                
                if (stats.retriesMin < merged.retriesMin) {
                    merged.retriesMin = stats.retriesMin;
                }
                if (stats.retriesMax > merged.retriesMax) {
                    merged.retriesMax = stats.retriesMax;
                }
                
                // Ledger creation min/max
                if (stats.ledgerCreateLatencyMinUs < merged.ledgerCreateLatencyMinUs) {
                    merged.ledgerCreateLatencyMinUs = stats.ledgerCreateLatencyMinUs;
                }
                if (stats.ledgerCreateLatencyMaxUs > merged.ledgerCreateLatencyMaxUs) {
                    merged.ledgerCreateLatencyMaxUs = stats.ledgerCreateLatencyMaxUs;
                }
                
                // Ledger close min/max
                if (stats.ledgerCloseLatencyMinUs < merged.ledgerCloseLatencyMinUs) {
                    merged.ledgerCloseLatencyMinUs = stats.ledgerCloseLatencyMinUs;
                }
                if (stats.ledgerCloseLatencyMaxUs > merged.ledgerCloseLatencyMaxUs) {
                    merged.ledgerCloseLatencyMaxUs = stats.ledgerCloseLatencyMaxUs;
                }
            }
            
            // Merge percentile histograms
            if (merged.trackPercentiles && stats.trackPercentiles) {
                synchronized (merged.majorityLatencyHistogram) {
                    synchronized (stats.majorityLatencyHistogram) {
                        for (Map.Entry<Long, Long> entry : stats.majorityLatencyHistogram.entrySet()) {
                            merged.majorityLatencyHistogram.merge(entry.getKey(), entry.getValue(), Long::sum);
                        }
                    }
                }
            }
        }
        
        return merged;
    }
    
    public Map<String, Object> toJson() {
        Map<String, Object> json = new HashMap<>();
        
        long reqCount = requestCount.sum();
        
        // Basic counts
        json.put("request_count", reqCount);
        json.put("ledger_create_count", ledgerCreateCount.sum());
        
        // Majority latency stats
        json.put("majority_latency_min_us", majorityLatencyMinUs == Long.MAX_VALUE ? 0 : majorityLatencyMinUs);
        json.put("majority_latency_max_us", majorityLatencyMaxUs == Long.MIN_VALUE ? 0 : majorityLatencyMaxUs);
        json.put("majority_latency_avg_us", reqCount > 0 ? totalMajorityLatencyUs.sum() / reqCount : 0);
        
        // Send latency stats
        json.put("send_latency_min_us", sendLatencyMinUs == Long.MAX_VALUE ? 0 : sendLatencyMinUs);
        json.put("send_latency_max_us", sendLatencyMaxUs == Long.MIN_VALUE ? 0 : sendLatencyMaxUs);
        json.put("send_latency_avg_us", reqCount > 0 ? totalSendLatencyUs.sum() / reqCount : 0);
        
        // Request size stats
        json.put("request_size_min", requestSizeMin == Long.MAX_VALUE ? 0 : requestSizeMin);
        json.put("request_size_max", requestSizeMax == Long.MIN_VALUE ? 0 : requestSizeMax);
        json.put("request_size_avg", reqCount > 0 ? (double)totalRequestSize.sum() / reqCount : 0.0);
        
        // Retry stats
        json.put("retries_min", retriesMin == Long.MAX_VALUE ? 0 : retriesMin);
        json.put("retries_max", retriesMax == Long.MIN_VALUE ? 0 : retriesMax);
        json.put("retries_avg", reqCount > 0 ? (double)totalRetries.sum() / reqCount : 0.0);
        
        // Ledger creation stats (if enabled)
        long createCount = ledgerCreateCount.sum();
        if (createCount > 0) {
            json.put("ledger_create_latency_min_us", ledgerCreateLatencyMinUs == Long.MAX_VALUE ? 0 : ledgerCreateLatencyMinUs);
            json.put("ledger_create_latency_max_us", ledgerCreateLatencyMaxUs == Long.MIN_VALUE ? 0 : ledgerCreateLatencyMaxUs);
            json.put("ledger_create_latency_avg_us", totalLedgerCreateLatencyUs.sum() / createCount);
        }
        
        // Ledger close stats (if enabled)
        long closeCount = ledgerCloseCount.sum();
        if (closeCount > 0) {
            json.put("ledger_close_count", closeCount);
            json.put("ledger_close_latency_min_us", ledgerCloseLatencyMinUs == Long.MAX_VALUE ? 0 : ledgerCloseLatencyMinUs);
            json.put("ledger_close_latency_max_us", ledgerCloseLatencyMaxUs == Long.MIN_VALUE ? 0 : ledgerCloseLatencyMaxUs);
            json.put("ledger_close_latency_avg_us", totalLedgerCloseLatencyUs.sum() / closeCount);
        }
        
        // Percentile stats (if enabled)
        if (trackPercentiles) {
            json.put("majority_latency_p50_us", calculatePercentile(50.0));
            json.put("majority_latency_p90_us", calculatePercentile(90.0));
            json.put("majority_latency_p95_us", calculatePercentile(95.0));
            json.put("majority_latency_p99_us", calculatePercentile(99.0));
            json.put("majority_latency_p999_us", calculatePercentile(99.9));
        }
        
        return json;
    }
    
    public String[] csvColumns() {
        long reqCount = requestCount.sum();
        
        List<String> columns = new ArrayList<>();
        columns.add(String.valueOf(reqCount));
        columns.add(String.valueOf(majorityLatencyMinUs == Long.MAX_VALUE ? 0 : majorityLatencyMinUs));
        columns.add(String.valueOf(majorityLatencyMaxUs == Long.MIN_VALUE ? 0 : majorityLatencyMaxUs));
        columns.add(String.valueOf(reqCount > 0 ? totalMajorityLatencyUs.sum() / reqCount : 0));
        columns.add(String.valueOf(sendLatencyMinUs == Long.MAX_VALUE ? 0 : sendLatencyMinUs));
        columns.add(String.valueOf(sendLatencyMaxUs == Long.MIN_VALUE ? 0 : sendLatencyMaxUs));
        columns.add(String.valueOf(reqCount > 0 ? totalSendLatencyUs.sum() / reqCount : 0));
        columns.add(String.valueOf(requestSizeMin == Long.MAX_VALUE ? 0 : requestSizeMin));
        columns.add(String.valueOf(requestSizeMax == Long.MIN_VALUE ? 0 : requestSizeMax));
        columns.add(String.valueOf(reqCount > 0 ? (double)totalRequestSize.sum() / reqCount : 0.0));
        columns.add(String.valueOf(retriesMin == Long.MAX_VALUE ? 0 : retriesMin));
        columns.add(String.valueOf(retriesMax == Long.MIN_VALUE ? 0 : retriesMax));
        columns.add(String.valueOf(reqCount > 0 ? (double)totalRetries.sum() / reqCount : 0.0));
        
        // Add percentile columns if tracking is enabled
        if (trackPercentiles) {
            columns.add(String.valueOf(calculatePercentile(50.0)));
            columns.add(String.valueOf(calculatePercentile(90.0)));
            columns.add(String.valueOf(calculatePercentile(95.0)));
            columns.add(String.valueOf(calculatePercentile(99.0)));
            columns.add(String.valueOf(calculatePercentile(99.9)));
        }
        
        return columns.toArray(new String[0]);
    }
    
    // Getters for individual stats
    public long getRequestCount() {
        return requestCount.sum();
    }
    
    public double getAvgMajorityLatencyUs() {
        long count = requestCount.sum();
        return count > 0 ? (double)totalMajorityLatencyUs.sum() / count : 0.0;
    }
    
    public double getAvgSendLatencyUs() {
        long count = requestCount.sum();
        return count > 0 ? (double)totalSendLatencyUs.sum() / count : 0.0;
    }
    
    public double getAvgRequestSize() {
        long count = requestCount.sum();
        return count > 0 ? (double)totalRequestSize.sum() / count : 0.0;
    }
    
    public double getAvgRetries() {
        long count = requestCount.sum();
        return count > 0 ? (double)totalRetries.sum() / count : 0.0;
    }
    
    /**
     * Calculate percentile from the latency histogram.
     * @param percentile The percentile to calculate (0-100)
     * @return The latency in microseconds at the given percentile
     */
    public long calculatePercentile(double percentile) {
        if (!trackPercentiles) {
            return 0;
        }
        
        long totalCount = requestCount.sum();
        if (totalCount == 0) {
            return 0;
        }
        
        long targetCount = (long) Math.ceil(totalCount * percentile / 100.0);
        long currentCount = 0;
        
        synchronized (majorityLatencyHistogram) {
            for (Map.Entry<Long, Long> entry : majorityLatencyHistogram.entrySet()) {
                currentCount += entry.getValue();
                if (currentCount >= targetCount) {
                    return entry.getKey();
                }
            }
        }
        
        return 0;
    }
    
    public boolean tracksPercentiles() {
        return trackPercentiles;
    }
}