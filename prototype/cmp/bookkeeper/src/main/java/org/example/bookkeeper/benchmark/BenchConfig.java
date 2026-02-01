package org.example.bookkeeper.benchmark;

import org.apache.commons.cli.*;

public class BenchConfig {
    // Workload parameters
    public final int opCount;
    public final double ledgerCreationProbability;
    public final int maxLedgerCount;
    public final int initialLedgerCount;
    public final int threadCount;
    public final int tasksPerThread;
    public final int minMessageSize;
    public final int maxMessageSize;
    public final boolean trackPercentiles;
    public final boolean trackLedgerLatency;
    
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

    private BenchConfig(Builder builder) {
        this.opCount = builder.opCount;
        this.ledgerCreationProbability = builder.ledgerCreationProbability;
        this.maxLedgerCount = builder.maxLedgerCount;
        this.initialLedgerCount = builder.initialLedgerCount;
        this.threadCount = builder.threadCount;
        this.tasksPerThread = builder.tasksPerThread;
        this.minMessageSize = builder.minMessageSize;
        this.maxMessageSize = builder.maxMessageSize;
        this.trackPercentiles = builder.trackPercentiles;
        this.trackLedgerLatency = builder.trackLedgerLatency;
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

    public static BenchConfig parseArgs(String[] args) {
        Options options = new Options();
        
        // Workload control options
        options.addOption(Option.builder()
            .longOpt("op-count")
            .hasArg()
            .desc("How many operations to execute overall")
            .build());
        
        options.addOption(Option.builder()
            .longOpt("ledger-creation-probability")
            .hasArg()
            .desc("Probability that an operation will be a ledger creation request")
            .build());
        
        options.addOption(Option.builder()
            .longOpt("max-ledger-count")
            .hasArg()
            .desc("Overall maximum ledger count after which no more ledgers are created")
            .build());
        
        options.addOption(Option.builder()
            .longOpt("initial-ledger-count")
            .hasArg()
            .desc("Number of initial ledgers to create per task")
            .build());
        
        options.addOption(Option.builder()
            .longOpt("thread-count")
            .hasArg()
            .desc("Number of parallel threads to run")
            .build());
        
        options.addOption(Option.builder()
            .longOpt("tasks-per-thread")
            .hasArg()
            .desc("Number of concurrent tasks to spawn per thread")
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
        
        // Performance tracking
        options.addOption(Option.builder()
            .longOpt("track-percentiles")
            .desc("Enable detailed latency percentile tracking")
            .build());
        
        options.addOption(Option.builder()
            .longOpt("track-ledger-latency")
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
                formatter.printHelp("bookkeeper-benchmark", options);
                System.exit(0);
            }
            
            return new Builder()
                .opCount(Integer.parseInt(cmd.getOptionValue("op-count", "1000000")))
                .ledgerCreationProbability(Double.parseDouble(cmd.getOptionValue("ledger-creation-probability", "0.000001")))
                .maxLedgerCount(Integer.parseInt(cmd.getOptionValue("max-ledger-count", "256")))
                .initialLedgerCount(Integer.parseInt(cmd.getOptionValue("initial-ledger-count", "1")))
                .threadCount(Integer.parseInt(cmd.getOptionValue("thread-count", "1")))
                .tasksPerThread(Integer.parseInt(cmd.getOptionValue("tasks-per-thread", "1")))
                .minMessageSize(Integer.parseInt(cmd.getOptionValue("min-message-size", "1")))
                .maxMessageSize(Integer.parseInt(cmd.getOptionValue("max-message-size", "8192")))
                .trackPercentiles(cmd.hasOption("track-percentiles"))
                .trackLedgerLatency(cmd.hasOption("track-ledger-latency"))
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
            formatter.printHelp("bookkeeper-benchmark", options);
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
        private double ledgerCreationProbability = 0.000001;
        private int maxLedgerCount = 256;
        private int initialLedgerCount = 1;
        private int threadCount = 1;
        private int tasksPerThread = 1;
        private int minMessageSize = 1;
        private int maxMessageSize = 8192;
        private boolean trackPercentiles = false;
        private boolean trackLedgerLatency = false;
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
        public Builder ledgerCreationProbability(double prob) { this.ledgerCreationProbability = prob; return this; }
        public Builder maxLedgerCount(int count) { this.maxLedgerCount = count; return this; }
        public Builder initialLedgerCount(int count) { this.initialLedgerCount = count; return this; }
        public Builder threadCount(int count) { this.threadCount = count; return this; }
        public Builder tasksPerThread(int count) { this.tasksPerThread = count; return this; }
        public Builder minMessageSize(int size) { this.minMessageSize = size; return this; }
        public Builder maxMessageSize(int size) { this.maxMessageSize = size; return this; }
        public Builder trackPercentiles(boolean track) { this.trackPercentiles = track; return this; }
        public Builder trackLedgerLatency(boolean track) { this.trackLedgerLatency = track; return this; }
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

        public BenchConfig build() {
            return new BenchConfig(this);
        }
    }

    @Override
    public String toString() {
        return String.format("BenchConfig{opCount=%d, threads=%d, tasks=%d, ensembleSize=%d, writeQuorum=%d, ackQuorum=%d}",
            opCount, threadCount, tasksPerThread, ensembleSize, writeQuorumSize, ackQuorumSize);
    }
}