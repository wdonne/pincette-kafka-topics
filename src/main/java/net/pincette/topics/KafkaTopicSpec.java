package net.pincette.topics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class KafkaTopicSpec {
  @JsonProperty("maxMessageBytes")
  public final int maxMessageBytes;

  @JsonProperty("name")
  public final String name;

  @JsonProperty("partitions")
  public final int partitions;

  @JsonProperty("replicationFactor")
  public final int replicationFactor;

  @JsonProperty("retentionBytes")
  public final int retentionBytes;

  @JsonProperty("retentionMilliseconds")
  public final int retentionMilliseconds;

  @JsonCreator
  public KafkaTopicSpec() {
    this(null, -1, -1, -1, -1, -1);
  }

  private KafkaTopicSpec(
      final String name,
      final int maxMessageBytes,
      final int partitions,
      final int replicationFactor,
      final int retentionBytes,
      final int retentionMilliseconds) {
    this.name = name;
    this.maxMessageBytes = maxMessageBytes;
    this.partitions = partitions;
    this.replicationFactor = replicationFactor;
    this.retentionBytes = retentionBytes;
    this.retentionMilliseconds = retentionMilliseconds;
  }

  KafkaTopicSpec withMaxMessageBytes(final int maxMessageBytes) {
    return new KafkaTopicSpec(
        name,
        maxMessageBytes,
        partitions,
        replicationFactor,
        retentionBytes,
        retentionMilliseconds);
  }

  KafkaTopicSpec withName(final String name) {
    return new KafkaTopicSpec(
        name,
        maxMessageBytes,
        partitions,
        replicationFactor,
        retentionBytes,
        retentionMilliseconds);
  }

  KafkaTopicSpec withPartitions(final int partitions) {
    return new KafkaTopicSpec(
        name,
        maxMessageBytes,
        partitions,
        replicationFactor,
        retentionBytes,
        retentionMilliseconds);
  }

  KafkaTopicSpec withReplicationFactor(final int replicationFactor) {
    return new KafkaTopicSpec(
        name,
        maxMessageBytes,
        partitions,
        replicationFactor,
        retentionBytes,
        retentionMilliseconds);
  }

  KafkaTopicSpec withRetentionBytes(final int retentionBytes) {
    return new KafkaTopicSpec(
        name,
        maxMessageBytes,
        partitions,
        replicationFactor,
        retentionBytes,
        retentionMilliseconds);
  }

  KafkaTopicSpec withRetentionMilliseconds(final int retentionMilliseconds) {
    return new KafkaTopicSpec(
        name,
        maxMessageBytes,
        partitions,
        replicationFactor,
        retentionBytes,
        retentionMilliseconds);
  }
}
