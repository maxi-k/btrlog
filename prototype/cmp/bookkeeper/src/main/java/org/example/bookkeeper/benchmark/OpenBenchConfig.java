package org.example.bookkeeper.benchmark;

import org.apache.commons.cli.*;

/**
 * Configuration for open benchmark (constant arrival rate).
 * Maps to the BenchConfig in open_bench.rs
 */
public class OpenBenchConfig {
    // Workload parameters
    public final int opCount;
    public final long logCreationIntervalMs;
    public final long minLogPushIntervalMs;
    public final long maxLogPushIntervalMs;
    public final long printIntervalMs;
    public final int primaryThreadCount;
    public final int initialLogCount;
    public final int minMessageSize;
    public final int maxMessageSize;
    public final double zipfFactor;
    public final boolean trackPercentiles;
    public final boolean trackJournalLatency;

    // BookKeeper specific
    public final int ensembleSize;
    public final int writeQuorumSize;
    public final int ackQuorumSize;
    public final String zkServers;
    public final int zkTimeout;

    // Timeout settings
    public final int createLedgerTimeoutMs;
    public final int addEntryTimeoutMs;
    public final int retries;
    public final double retryBackoffFactor;
    
    // Logging
    public final String logLevel;

    private OpenBenchConfig(Builder builder) {
        this.opCount = builder.opCount;
        this.logCreationIntervalMs = builder.logCreationIntervalMs;
        this.minLogPushIntervalMs = builder.minLogPushIntervalMs;
        this.maxLogPushIntervalMs = builder.maxLogPushIntervalMs;
        this.printIntervalMs = builder.printIntervalMs;
        this.primaryThreadCount = builder.primaryThreadCount;
        this.initialLogCount = builder.initialLogCount;
        this.minMessageSize = builder.minMessageSize;
        this.maxMessageSize = builder.maxMessageSize;
        this.zipfFactor = builder.zipfFactor;
        this.trackPercentiles = builder.trackPercentiles;
        this.trackJournalLatency = builder.trackJournalLatency;
        this.ensembleSize = builder.ensembleSize;
        this.writeQuorumSize = builder.writeQuorumSize;
        this.ackQuorumSize = builder.ackQuorumSize;
        this.zkServers = builder.zkServers;
        this.zkTimeout = builder.zkTimeout;
        this.createLedgerTimeoutMs = builder.createLedgerTimeoutMs;
        this.addEntryTimeoutMs = builder.addEntryTimeoutMs;
        this.retries = builder.retries;
        this.retryBackoffFactor = builder.retryBackoffFactor;
        this.logLevel = builder.logLevel;
    }

    public static OpenBenchConfig parseArgs(String[] args) {
        Options options = new Options();

        // Workload control options
        options.addOption(Option.builder()
            .longOpt("op-count")
            .hasArg()
            .desc("How many operations to execute overall")
            .build());

        options.addOption(Option.builder()
            .longOpt("log-creation-interval-ms")
            .hasArg()
            .desc("After how many milliseconds should we create new logs with their own arrival rate")
            .build());

        options.addOption(Option.builder()
            .longOpt("min-log-push-interval-ms")
            .hasArg()
            .desc("Minimum interval between pushes to a single log (bounded by response time)")
            .build());

        options.addOption(Option.builder()
            .longOpt("max-log-push-interval-ms")
            .hasArg()
            .desc("Maximum interval for the push interval random distribution")
            .build());

        options.addOption(Option.builder()
            .longOpt("print-interval-ms")
            .hasArg()
            .desc("How often to print latency percentiles and op count")
            .build());

        options.addOption(Option.builder()
            .longOpt("primary-thread-count")
            .hasArg()
            .desc("Number of parallel benchmark instances (threads) to run")
            .build());

        options.addOption(Option.builder()
            .longOpt("initial-log-count")
            .hasArg()
            .desc("Number of initial ledgers to create per thread")
            .build());

        options.addOption(Option.builder()
            .longOpt("min-message-size")
            .hasArg()
            .desc("Minimum message payload size")
            .build());

        options.addOption(Option.builder()
            .longOpt("max-message-size")
            .hasArg()
            .desc("Maximum message payload size")
            .build());

        options.addOption(Option.builder()
            .longOpt("zipf-factor")
            .hasArg()
            .desc("Zipf exponent for size distribution (higher = more skewed towards smaller sizes)")
            .build());

        // Performance tracking
        options.addOption(Option.builder()
            .longOpt("track-percentiles")
            .desc("Enable detailed latency percentile tracking")
            .build());

        options.addOption(Option.builder()
            .longOpt("track-journal-latency")
            .desc("Track ledger creation latency")
            .build());

        // BookKeeper specific
        options.addOption(Option.builder()
            .longOpt("ensemble-size")
            .hasArg()
            .desc("BookKeeper ensemble size")
            .build());

        options.addOption(Option.builder()
            .longOpt("write-quorum-size")
            .hasArg()
            .desc("BookKeeper write quorum size")
            .build());

        options.addOption(Option.builder()
            .longOpt("ack-quorum-size")
            .hasArg()
            .desc("BookKeeper ack quorum size")
            .build());

        options.addOption(Option.builder()
            .longOpt("zk-servers")
            .hasArg()
            .desc("ZooKeeper connection string")
            .build());

        // Timeout and retry settings
        options.addOption(Option.builder()
            .longOpt("create-ledger-timeout-ms")
            .hasArg()
            .desc("Timeout for ledger creation operations")
            .build());

        options.addOption(Option.builder()
            .longOpt("add-entry-timeout-ms")
            .hasArg()
            .desc("Timeout for add entry operations")
            .build());

        options.addOption(Option.builder()
            .longOpt("retries")
            .hasArg()
            .desc("Number of retries for failed operations")
            .build());

        options.addOption(Option.builder()
            .longOpt("retry-backoff-factor")
            .hasArg()
            .desc("Backoff factor for retries")
            .build());

        options.addOption(Option.builder()
            .longOpt("log-level")
            .hasArg()
            .desc("Log level (TRACE, DEBUG, INFO, WARN, ERROR)")
            .build());

        options.addOption("h", "help", false, "Show help");

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("bookkeeper-open-benchmark", options);
                System.exit(0);
            }

            return new Builder()
                .opCount(Integer.parseInt(cmd.getOptionValue("op-count", "1000000")))
                .logCreationIntervalMs(Long.parseLong(cmd.getOptionValue("log-creation-interval-ms", "500")))
                .minLogPushIntervalMs(Long.parseLong(cmd.getOptionValue("min-log-push-interval-ms", "1")))
                .maxLogPushIntervalMs(Long.parseLong(cmd.getOptionValue("max-log-push-interval-ms", "10")))
                .printIntervalMs(Long.parseLong(cmd.getOptionValue("print-interval-ms", "1000")))
                .primaryThreadCount(Integer.parseInt(cmd.getOptionValue("primary-thread-count", "1")))
                .initialLogCount(Integer.parseInt(cmd.getOptionValue("initial-log-count", "1")))
                .minMessageSize(Integer.parseInt(cmd.getOptionValue("min-message-size", "1")))
                .maxMessageSize(Integer.parseInt(cmd.getOptionValue("max-message-size", "8192")))
                .zipfFactor(Double.parseDouble(cmd.getOptionValue("zipf-factor", "0.9")))
                .trackPercentiles(cmd.hasOption("track-percentiles"))
                .trackJournalLatency(cmd.hasOption("track-journal-latency"))
                .ensembleSize(Integer.parseInt(cmd.getOptionValue("ensemble-size", "3")))
                .writeQuorumSize(Integer.parseInt(cmd.getOptionValue("write-quorum-size", "3")))
                .ackQuorumSize(Integer.parseInt(cmd.getOptionValue("ack-quorum-size", "2")))
                .zkServers(cmd.getOptionValue("zk-servers", getDefaultZkServers()))
                .zkTimeout(30000)
                .createLedgerTimeoutMs(Integer.parseInt(cmd.getOptionValue("create-ledger-timeout-ms", "5000")))
                .addEntryTimeoutMs(Integer.parseInt(cmd.getOptionValue("add-entry-timeout-ms", "1000")))
                .retries(Integer.parseInt(cmd.getOptionValue("retries", "3")))
                .retryBackoffFactor(Double.parseDouble(cmd.getOptionValue("retry-backoff-factor", "2.0")))
                .logLevel(cmd.getOptionValue("log-level", "INFO"))
                .build();

        } catch (ParseException e) {
            System.err.println("Error parsing command line: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("bookkeeper-open-benchmark", options);
            System.exit(1);
            return null;
        }
    }

    private static String getDefaultZkServers() {
        // Try to build ZK connect string from cluster IPs
        String clusterSize = System.getenv("JOURNAL_CLUSTER_SIZE");
        if (clusterSize != null) {
            StringBuilder zkServers = new StringBuilder();
            int size = Integer.parseInt(clusterSize);

            for (int i = 0; i < size - 1; i++) { // Exclude primary node
                String ip = System.getenv("JOURNAL_NODE_IP_" + i);
                if (ip != null) {
                    if (zkServers.length() > 0) {
                        zkServers.append(",");
                    }
                    zkServers.append(ip).append(":2181");
                }
            }

            if (zkServers.length() > 0) {
                return zkServers.toString();
            }
        }

        // Fallback to localhost for local testing
        return "localhost:2181";
    }

    public static class Builder {
        private int opCount = 1000000;
        private long logCreationIntervalMs = 200;
        private long minLogPushIntervalMs = 1;
        private long maxLogPushIntervalMs = 10;
        private long printIntervalMs = 200;
        private int primaryThreadCount = 1;
        private int initialLogCount = 1;
        private int minMessageSize = 1;
        private int maxMessageSize = 8192;
        private double zipfFactor = 0.9;
        private boolean trackPercentiles = false;
        private boolean trackJournalLatency = false;
        private int ensembleSize = 3;
        private int writeQuorumSize = 3;
        private int ackQuorumSize = 2;
        private String zkServers = "localhost:2181";
        private int zkTimeout = 30000;
        private int createLedgerTimeoutMs = 5000;
        private int addEntryTimeoutMs = 1000;
        private int retries = 3;
        private double retryBackoffFactor = 2.0;
        private String logLevel = "INFO";

        public Builder opCount(int opCount) { this.opCount = opCount; return this; }
        public Builder logCreationIntervalMs(long ms) { this.logCreationIntervalMs = ms; return this; }
        public Builder minLogPushIntervalMs(long ms) { this.minLogPushIntervalMs = ms; return this; }
        public Builder maxLogPushIntervalMs(long ms) { this.maxLogPushIntervalMs = ms; return this; }
        public Builder printIntervalMs(long ms) { this.printIntervalMs = ms; return this; }
        public Builder primaryThreadCount(int count) { this.primaryThreadCount = count; return this; }
        public Builder initialLogCount(int count) { this.initialLogCount = count; return this; }
        public Builder minMessageSize(int size) { this.minMessageSize = size; return this; }
        public Builder maxMessageSize(int size) { this.maxMessageSize = size; return this; }
        public Builder zipfFactor(double factor) { this.zipfFactor = factor; return this; }
        public Builder trackPercentiles(boolean track) { this.trackPercentiles = track; return this; }
        public Builder trackJournalLatency(boolean track) { this.trackJournalLatency = track; return this; }
        public Builder ensembleSize(int size) { this.ensembleSize = size; return this; }
        public Builder writeQuorumSize(int size) { this.writeQuorumSize = size; return this; }
        public Builder ackQuorumSize(int size) { this.ackQuorumSize = size; return this; }
        public Builder zkServers(String servers) { this.zkServers = servers; return this; }
        public Builder zkTimeout(int timeout) { this.zkTimeout = timeout; return this; }
        public Builder createLedgerTimeoutMs(int timeout) { this.createLedgerTimeoutMs = timeout; return this; }
        public Builder addEntryTimeoutMs(int timeout) { this.addEntryTimeoutMs = timeout; return this; }
        public Builder retries(int retries) { this.retries = retries; return this; }
        public Builder retryBackoffFactor(double factor) { this.retryBackoffFactor = factor; return this; }
        public Builder logLevel(String level) { this.logLevel = level; return this; }

        public OpenBenchConfig build() {
            return new OpenBenchConfig(this);
        }
    }

    @Override
    public String toString() {
        return String.format("OpenBenchConfig{opCount=%d, threads=%d, initialLogs=%d, ensembleSize=%d, writeQuorum=%d, ackQuorum=%d}",
            opCount, primaryThreadCount, initialLogCount, ensembleSize, writeQuorumSize, ackQuorumSize);
    }
}
