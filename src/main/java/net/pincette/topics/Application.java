package net.pincette.topics;

import static net.pincette.util.Util.initLogging;

import io.javaoperatorsdk.operator.Operator;

public class Application {

  public static void main(final String[] args) {
    final Operator operator = new Operator();

    initLogging();
    operator.register(new KafkaTopicReconciler());
    operator.start();
  }
}
