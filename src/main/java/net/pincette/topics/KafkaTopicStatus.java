package net.pincette.topics;


import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import net.pincette.operator.util.Status;

public class KafkaTopicStatus extends Status {
  @JsonProperty("messageLag")
  public Map<String, Map<String, Long>> messageLag;

  KafkaTopicStatus withMessageLag(final Map<String, Map<String, Long>> messageLag) {
    this.messageLag = messageLag;

    return this;
  }
}
