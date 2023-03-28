package net.pincette.topics;

import static com.mongodb.assertions.Assertions.assertTrue;
import static io.fabric8.kubernetes.client.Config.autoConfigure;
import static java.lang.String.valueOf;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.jes.util.Configuration.loadDefault;
import static net.pincette.jes.util.Kafka.fromConfig;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.operator.testutil.Util.createNamespace;
import static net.pincette.operator.testutil.Util.createOrReplaceAndWait;
import static net.pincette.operator.testutil.Util.deleteAndWait;
import static net.pincette.operator.testutil.Util.deleteNamespace;
import static net.pincette.topics.KafkaTopicReconciler.MAX_MESSAGE_BYTES;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.waitFor;
import static net.pincette.util.Util.waitForCondition;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.javaoperatorsdk.operator.Operator;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import net.pincette.operator.testutil.Util;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestKafkaTopics {
  private static final KubernetesClient CLIENT =
      new KubernetesClientBuilder().withConfig(autoConfigure("minikube")).build();
  private static final Map<String, Object> COMMON_CONFIG = fromConfig(loadDefault());
  private static final String CONSUMER_GROUP = "test-consumer-group";
  private static final Map<String, Object> CONSUMER_CONFIG =
      merge(
          COMMON_CONFIG,
          map(
              pair(GROUP_ID_CONFIG, CONSUMER_GROUP),
              pair(ENABLE_AUTO_COMMIT_CONFIG, false),
              pair(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
              pair(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)));
  private static final Admin ADMIN = Admin.create(COMMON_CONFIG);
  private static final KafkaConsumer<String, String> CONSUMER =
      new KafkaConsumer<>(CONSUMER_CONFIG);
  private static final Duration INTERVAL = ofSeconds(1);
  private static final String NAMESPACE = "test-kafka-topics";
  private static final Map<String, Object> PRODUCER_CONFIG =
      merge(
          COMMON_CONFIG,
          map(
              pair(ACKS_CONFIG, "all"),
              pair(ENABLE_IDEMPOTENCE_CONFIG, true),
              pair(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
              pair(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)));
  private static final String TOPIC = "test-topic";
  private static final KafkaProducer<String, String> PRODUCER =
      new KafkaProducer<>(PRODUCER_CONFIG);

  private static CompletionStage<Boolean> consumeOnce() {
    final CompletableFuture<Boolean> future = new CompletableFuture<>();

    CONSUMER.subscribe(
        set(TOPIC),
        new ConsumerRebalanceListener() {
          public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            CONSUMER.seekToBeginning(CONSUMER.assignment());
          }

          public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
        });

    new Thread(
            () -> {
              while (CONSUMER.poll(ofMillis(100)).isEmpty())
                ;
              CONSUMER.commitSync();
              future.complete(true);
            })
        .start();

    return future;
  }

  private static KafkaTopic createTopic(final String name, final String namespace) {
    return createTopic(name, namespace, new KafkaTopicSpec().withMaxMessageBytes(1000000));
  }

  private static KafkaTopic createTopic(
      final String name, final String namespace, final KafkaTopicSpec spec) {
    final KafkaTopic topic = new KafkaTopic();

    topic.setMetadata(new ObjectMetaBuilder().withName(name).withNamespace(namespace).build());
    topic.setSpec(spec.withName(name).withPartitions(1).withReplicationFactor(1));

    return topic;
  }

  private static void createTopic(final KafkaTopic topic) {
    createOrReplaceAndWait(CLIENT.resources(KafkaTopic.class).resource(topic));
  }

  @AfterAll
  public static void deleteAll() {
    deleteKafkaTopic(TOPIC, NAMESPACE);
    deleteCustomResource();
    deleteNamespace(CLIENT, NAMESPACE);
  }

  private static void deleteCustomResource() {
    Util.deleteCustomResource(CLIENT, "kafkatopics.pincette.net");
  }

  private static void deleteKafkaTopic(final String name, final String namespace) {
    deleteAndWait(CLIENT.resources(KafkaTopic.class).inNamespace(namespace).withName(name));
  }

  private static CompletionStage<Boolean> describeConfigs(
      final String topic, final Function<Config, Boolean> test, final Admin admin) {
    final ConfigResource key = new ConfigResource(Type.TOPIC, topic);

    return admin
        .describeConfigs(set(key))
        .all()
        .toCompletionStage()
        .thenApply(m -> m.get(key))
        .thenApply(test)
        .exceptionally(e -> false);
  }

  private static CompletionStage<Boolean> describeTopic(
      final String topic,
      final Function<TopicDescription, Boolean> found,
      final BooleanSupplier notFound,
      final Admin admin) {
    return admin
        .describeTopics(set(topic))
        .allTopicNames()
        .toCompletionStage()
        .thenApply(m -> m.get(topic))
        .thenApply(found)
        .exceptionally(e -> notFound.getAsBoolean());
  }

  private static KafkaTopic getTopic(final String name, final String namespace) {
    return CLIENT.resources(KafkaTopic.class).inNamespace(namespace).withName(name).get();
  }

  private static boolean hasMessageLag(final KafkaTopic topic, final int messageLag) {
    return ofNullable(topic.getStatus())
        .map(s -> s.messageLag)
        .map(m -> m.get(CONSUMER_GROUP))
        .map(g -> g.get("0"))
        .filter(lag -> lag == messageLag)
        .isPresent();
  }

  private static void loadCustomResource() {
    Util.loadCustomResource(
        CLIENT,
        KafkaTopic.class.getResourceAsStream("/META-INF/fabric8/kafkatopics.pincette.net-v1.yml"));
  }

  @BeforeAll
  public static void prepare() {
    loadCustomResource();
    createNamespace(CLIENT, NAMESPACE);
    startOperator();
  }

  private static CompletionStage<Boolean> produce() {
    return send(PRODUCER, new ProducerRecord<>(TOPIC, "test", "test"));
  }

  private static void startOperator() {
    final Operator operator = new Operator(CLIENT);

    operator.register(new KafkaTopicReconciler());
    operator.start();
  }

  private static CompletionStage<Boolean> topicAbsent(final String topic, final Admin admin) {
    return describeTopic(topic, d -> false, () -> true, admin);
  }

  private static CompletionStage<Boolean> topicPresent(final String topic, final Admin admin) {
    return describeTopic(topic, d -> true, () -> false, admin);
  }

  @AfterEach
  void after() {
    ADMIN.deleteTopics(set(TOPIC));
    waitFor(waitForCondition(() -> topicAbsent(TOPIC, ADMIN)), INTERVAL)
        .toCompletableFuture()
        .join();
  }

  @BeforeEach
  void before() {
    deleteKafkaTopic(TOPIC, NAMESPACE);
    createTopic(createTopic(TOPIC, NAMESPACE));
    waitFor(waitForCondition(() -> topicPresent(TOPIC, ADMIN)), INTERVAL)
        .toCompletableFuture()
        .join();
  }

  @Test
  @DisplayName("change topic")
  void changeTopic() {
    createTopic(createTopic(TOPIC, NAMESPACE, new KafkaTopicSpec().withMaxMessageBytes(1000)));

    assertTrue(
        ADMIN
            .describeConfigs(set(new ConfigResource(Type.TOPIC, TOPIC)))
            .all()
            .toCompletionStage()
            .toCompletableFuture()
            .join()
            .values()
            .stream()
            .findFirst()
            .map(c -> c.get(MAX_MESSAGE_BYTES).value())
            .map(Integer::parseInt)
            .filter(m -> m == 1000)
            .isPresent());
  }

  @Test
  @DisplayName("create topic")
  void createTopic() {
    assertTrue(
        ADMIN
            .describeTopics(set(TOPIC))
            .allTopicNames()
            .toCompletionStage()
            .toCompletableFuture()
            .join()
            .containsKey(TOPIC));
  }

  @Test
  @DisplayName("illegal change topic")
  void illegalChangeTopic() {
    ADMIN
        .incrementalAlterConfigs(
            map(
                pair(
                    new ConfigResource(Type.TOPIC, TOPIC),
                    set(
                        new AlterConfigOp(
                            new ConfigEntry(MAX_MESSAGE_BYTES, valueOf(1000)), SET)))))
        .all()
        .toCompletionStage()
        .toCompletableFuture()
        .join();

    assertTrue(
        waitFor(
                waitForCondition(
                    () ->
                        describeConfigs(
                            TOPIC, c -> c.get(MAX_MESSAGE_BYTES).value().equals("1000000"), ADMIN)),
                INTERVAL)
            .toCompletableFuture()
            .join());
  }

  @Test
  @DisplayName("message lag")
  void messageLag() {
    final CompletionStage<Boolean> consumed = consumeOnce();

    produce()
        .thenComposeAsync(r -> consumed)
        .thenComposeAsync(r -> produce())
        .toCompletableFuture()
        .join();

    assertTrue(
        waitFor(
                waitForCondition(
                    () -> completedFuture(hasMessageLag(getTopic(TOPIC, NAMESPACE), 1))),
                INTERVAL)
            .toCompletableFuture()
            .join());
  }
}
