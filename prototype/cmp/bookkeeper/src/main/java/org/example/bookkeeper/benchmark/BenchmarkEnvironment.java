package org.example.bookkeeper.benchmark;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Collects environment metadata for benchmark results.
 * Equivalent to BenchmarkEnvironment in the Rust benchmark.
 */
public class BenchmarkEnvironment {
    @JsonProperty("cluster_size")
    public final String clusterSize;
    
    @JsonProperty("journal_platform") 
    public final String platform;
    
    @JsonProperty("logger_instance_type")
    public final String loggerInstanceType;
    
    @JsonProperty("primary_instance_type")
    public final String primaryInstanceType;
    
    @JsonProperty("placement_group")
    public final String placementGroup;
    
    @JsonProperty("placement_partition")
    public final String placementPartition;
    
    @JsonProperty("aws_az_id")
    public final String awsAzId;
    
    @JsonProperty("aws_region")
    public final String awsRegion;
    
    @JsonProperty("timestamp")
    public final String timestamp;
    
    // BookKeeper specific fields
    @JsonProperty("bookkeeper_version")
    public final String bookKeeperVersion;
    
    @JsonProperty("ensemble_size")
    public final int ensembleSize;
    
    @JsonProperty("write_quorum_size")
    public final int writeQuorumSize;
    
    @JsonProperty("ack_quorum_size") 
    public final int ackQuorumSize;

    private BenchmarkEnvironment(Builder builder) {
        this.clusterSize = builder.clusterSize;
        this.platform = builder.platform;
        this.loggerInstanceType = builder.loggerInstanceType;
        this.primaryInstanceType = builder.primaryInstanceType;
        this.placementGroup = builder.placementGroup;
        this.placementPartition = builder.placementPartition;
        this.awsAzId = builder.awsAzId;
        this.awsRegion = builder.awsRegion;
        this.timestamp = builder.timestamp;
        this.bookKeeperVersion = builder.bookKeeperVersion;
        this.ensembleSize = builder.ensembleSize;
        this.writeQuorumSize = builder.writeQuorumSize;
        this.ackQuorumSize = builder.ackQuorumSize;
    }
    
    /**
     * Collect environment information from system properties and environment variables.
     * Mirrors the get_benchmark_environment() function from the Rust benchmark.
     */
    public static BenchmarkEnvironment collect(BenchConfig config) {
        long timestampSeconds = System.currentTimeMillis() / 1000;
        
        return new Builder()
            .clusterSize(getEnvOrDefault("JOURNAL_CLUSTER_SIZE", "unknown"))
            .platform("bookkeeper")
            .loggerInstanceType(getEnvOrDefault("JOURNAL_LOGGER_INSTANCE", "unknown"))
            .primaryInstanceType(getEnvOrDefault("JOURNAL_PRIMARY_INSTANCE", "unknown"))
            .placementGroup(getEnvOrDefault("AWS_PLACEMENT_GROUP", "unknown"))
            .placementPartition(getEnvOrDefault("AWS_PLACEMENT_PARTITION", "unknown"))
            .awsAzId(getEnvOrDefault("AWS_AZ_ID", "unknown"))
            .awsRegion(getEnvOrDefault("AWS_REGION", "unknown"))
            .timestamp(String.valueOf(timestampSeconds))
            .bookKeeperVersion(getBookKeeperVersion())
            .ensembleSize(config.ensembleSize)
            .writeQuorumSize(config.writeQuorumSize)
            .ackQuorumSize(config.ackQuorumSize)
            .build();
    }
    
    private static String getEnvOrDefault(String envVar, String defaultValue) {
        String value = System.getenv(envVar);
        return value != null ? value : defaultValue;
    }
    
    private static String getBookKeeperVersion() {
        // Try to get BookKeeper version from package info
        try {
            Package pkg = org.apache.bookkeeper.client.BookKeeper.class.getPackage();
            String version = pkg.getImplementationVersion();
            return version != null ? version : "unknown";
        } catch (Exception e) {
            return "unknown";
        }
    }
    
    public static class Builder {
        private String clusterSize = "unknown";
        private String platform = "bookkeeper";
        private String loggerInstanceType = "unknown";
        private String primaryInstanceType = "unknown";
        private String placementGroup = "unknown";
        private String placementPartition = "unknown";
        private String awsAzId = "unknown";
        private String awsRegion = "unknown";
        private String timestamp = String.valueOf(System.currentTimeMillis() / 1000);
        private String bookKeeperVersion = "unknown";
        private int ensembleSize = 3;
        private int writeQuorumSize = 3;
        private int ackQuorumSize = 2;
        
        public Builder clusterSize(String clusterSize) { this.clusterSize = clusterSize; return this; }
        public Builder platform(String platform) { this.platform = platform; return this; }
        public Builder loggerInstanceType(String type) { this.loggerInstanceType = type; return this; }
        public Builder primaryInstanceType(String type) { this.primaryInstanceType = type; return this; }
        public Builder placementGroup(String group) { this.placementGroup = group; return this; }
        public Builder placementPartition(String partition) { this.placementPartition = partition; return this; }
        public Builder awsAzId(String azId) { this.awsAzId = azId; return this; }
        public Builder awsRegion(String region) { this.awsRegion = region; return this; }
        public Builder timestamp(String timestamp) { this.timestamp = timestamp; return this; }
        public Builder bookKeeperVersion(String version) { this.bookKeeperVersion = version; return this; }
        public Builder ensembleSize(int size) { this.ensembleSize = size; return this; }
        public Builder writeQuorumSize(int size) { this.writeQuorumSize = size; return this; }
        public Builder ackQuorumSize(int size) { this.ackQuorumSize = size; return this; }
        
        public BenchmarkEnvironment build() {
            return new BenchmarkEnvironment(this);
        }
    }
}