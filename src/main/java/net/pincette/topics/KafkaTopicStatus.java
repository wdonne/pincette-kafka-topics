package net.pincette.topics;

import static java.util.Collections.emptyMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.fabric8.kubernetes.model.annotation.PrinterColumn;
import java.util.List;
import java.util.Map;
import net.pincette.operator.util.Status;
import net.pincette.operator.util.Status.Condition;
import net.pincette.operator.util.Status.Health;

public class KafkaTopicStatus {
  @JsonProperty("conditions")
  public final List<Condition> conditions;

  @JsonProperty("health")
  public final Health health;

  @JsonProperty("messageLag")
  public final Map<String, Map<String, Long>> messageLag;

  @JsonProperty("phase")
  @PrinterColumn(name = "Phase")
  public final String phase;

  @JsonIgnore private final Status status;

  public KafkaTopicStatus() {
    this(new Status(), emptyMap());
  }

  private KafkaTopicStatus(final Status status, final Map<String, Map<String, Long>> messageLag) {
    this.status = status;
    this.messageLag = messageLag;
    conditions = status.conditions;
    health = status.health;
    phase = status.phase;
  }

  KafkaTopicStatus withException(final Throwable e) {
    return new KafkaTopicStatus(status.withException(e), messageLag);
  }

  KafkaTopicStatus withMessageLag(final Map<String, Map<String, Long>> messageLag) {
    return new KafkaTopicStatus(status.withCondition(new Condition()), messageLag);
  }
}
