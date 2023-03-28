package net.pincette.topics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.fabric8.generator.annotation.Required;

public class KafkaTopicSpec {
  @JsonProperty("maxMessageBytes")
  public final int maxMessageBytes;

  @JsonProperty("name")
  @Required
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
    this(-1, null, -1, -1, -1, -1);
  }

  private KafkaTopicSpec(
      final int maxMessageBytes,
      final String name,
      final int partitions,
      final int replicationFactor,
      final int retentionBytes,
      final int retentionMilliseconds) {
    this.maxMessageBytes = maxMessageBytes;
    this.name = name;
    this.partitions = partitions;
    this.replicationFactor = replicationFactor;
    this.retentionBytes = retentionBytes;
    this.retentionMilliseconds = retentionMilliseconds;
  }

  KafkaTopicSpec withMaxMessageBytes(final int maxMessageBytes) {
    return new KafkaTopicSpec(
        maxMessageBytes,
        name,
        partitions,
        replicationFactor,
        retentionBytes,
        retentionMilliseconds);
  }

  KafkaTopicSpec withName(final String name) {
    return new KafkaTopicSpec(
        maxMessageBytes,
        name,
        partitions,
        replicationFactor,
        retentionBytes,
        retentionMilliseconds);
  }

  KafkaTopicSpec withPartitions(final int partitions) {
    return new KafkaTopicSpec(
        maxMessageBytes,
        name,
        partitions,
        replicationFactor,
        retentionBytes,
        retentionMilliseconds);
  }

  KafkaTopicSpec withReplicationFactor(final int replicationFactor) {
    return new KafkaTopicSpec(
        maxMessageBytes,
        name,
        partitions,
        replicationFactor,
        retentionBytes,
        retentionMilliseconds);
  }

  KafkaTopicSpec withRetentionBytes(final int retentionBytes) {
    return new KafkaTopicSpec(
        maxMessageBytes,
        name,
        partitions,
        replicationFactor,
        retentionBytes,
        retentionMilliseconds);
  }

  KafkaTopicSpec withRetentionMilliseconds(final int retentionMilliseconds) {
    return new KafkaTopicSpec(
        maxMessageBytes,
        name,
        partitions,
        replicationFactor,
        retentionBytes,
        retentionMilliseconds);
  }
}
