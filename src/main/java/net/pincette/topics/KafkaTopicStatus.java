package net.pincette.topics;

import static net.pincette.topics.Phase.Pending;
import static net.pincette.topics.Phase.Ready;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.fabric8.generator.annotation.Required;
import java.util.Map;

public class KafkaTopicStatus {
  @JsonProperty("error")
  public final String error;

  @JsonProperty("messageLag")
  public final Map<String, Map<String, Long>> messageLag;

  @JsonProperty("phase")
  @Required
  public final Phase phase;

  @JsonCreator
  public KafkaTopicStatus() {
    this(Ready, null, null);
  }

  KafkaTopicStatus(final Map<String, Map<String, Long>> messageLag) {
    this(Ready, messageLag, null);
  }

  KafkaTopicStatus(final String error) {
    this(Pending, null, error);
  }

  private KafkaTopicStatus(
      final Phase phase, final Map<String, Map<String, Long>> messageLag, final String error) {
    this.phase = phase;
    this.messageLag = messageLag;
    this.error = error;
  }
}
