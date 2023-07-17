package net.pincette.topics;

import static com.typesafe.config.ConfigFactory.defaultOverrides;
import static io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer.generateNameFor;
import static io.javaoperatorsdk.operator.api.reconciler.UpdateControl.patchStatus;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.valueOf;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Logger.getLogger;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static net.pincette.jes.util.Configuration.loadDefault;
import static net.pincette.jes.util.Kafka.adminConfig;
import static net.pincette.jes.util.Kafka.fromConfig;
import static net.pincette.util.Collections.filterMap;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.concat;
import static net.pincette.util.Util.tryToGetWith;
import static org.apache.kafka.clients.admin.Admin.create;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.timer.TimerEventSource;
import io.javaoperatorsdk.operator.processing.retry.GradualRetry;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.logging.Logger;
import java.util.stream.Stream;
import net.pincette.jes.util.Kafka;
import net.pincette.operator.util.Status.Condition;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

@io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration
@GradualRetry(maxAttempts = MAX_VALUE)
public class KafkaTopicReconciler
    implements Reconciler<KafkaTopic>, EventSourceInitializer<KafkaTopic> {
  private static final String DEFAULT_PARTITIONS = "defaultPartitions";
  private static final String DEFAULT_REPLICATION_FACTOR = "defaultReplicationFactor";
  private static final Logger LOGGER = getLogger("net.pincette.topics");
  static final String MAX_MESSAGE_BYTES = "max.message.bytes";
  private static final String RETENTION_BYTES = "retention.bytes";
  private static final String RETENTION_MS = "retention.ms";

  private final TimerEventSource<KafkaTopic> timerEventSource = new TimerEventSource<>();

  private static Collection<AlterConfigOp> alterConfigs(final KafkaTopicSpec spec) {
    return properties(spec).entrySet().stream()
        .map(e -> new ConfigEntry(e.getKey(), e.getValue()))
        .map(c -> new AlterConfigOp(c, SET))
        .collect(toList());
  }

  private static boolean anyChanged(final KafkaTopicSpec oldSpec, final KafkaTopicSpec newSpec) {
    return (newSpec.maxMessageBytes != -1 && oldSpec.maxMessageBytes != newSpec.maxMessageBytes)
        || (newSpec.retentionBytes != -1 && oldSpec.retentionBytes != newSpec.retentionBytes)
        || (newSpec.retentionMilliseconds != -1
            && oldSpec.retentionMilliseconds != newSpec.retentionMilliseconds);
  }

  private static CompletionStage<KafkaTopicSpec> createTopic(
      final String name,
      final KafkaTopicSpec spec,
      final Admin admin,
      final Map<String, Object> config) {
    LOGGER.info(() -> "Create topic " + name);

    return admin
        .createTopics(
            set(
                new NewTopic(
                        name,
                        defaultValue(spec.partitions, config, DEFAULT_PARTITIONS),
                        (short)
                            defaultValue(
                                spec.replicationFactor, config, DEFAULT_REPLICATION_FACTOR))
                    .configs(properties(spec))))
        .all()
        .toCompletionStage()
        .thenApply(r -> spec);
  }

  private static int defaultValue(
      final int value, final Map<String, Object> config, final String field) {
    return value != -1
        ? value
        : ofNullable(config.get(field))
            .filter(Integer.class::isInstance)
            .map(Integer.class::cast)
            .orElse(-1);
  }

  private static Map<String, Object> defaultsConfig(final Map<String, Object> config) {
    return filterMap(
        config,
        e ->
            DEFAULT_PARTITIONS.equals(e.getKey()) || DEFAULT_REPLICATION_FACTOR.equals(e.getKey()));
  }

  private static Map<String, Object> getConfig() {
    final Map<String, Object> config = fromConfig(defaultOverrides().withFallback(loadDefault()));

    return merge(adminConfig(config), defaultsConfig(config));
  }

  private static CompletionStage<Optional<KafkaTopicSpec>> getTopic(
      final String name, final Admin admin) {
    return admin
        .describeTopics(list(name))
        .allTopicNames()
        .toCompletionStage()
        .thenApply(r -> ofNullable(r.get(name)))
        .thenApply(
            description ->
                description.map(
                    d ->
                        new KafkaTopicSpec()
                            .withPartitions(d.partitions().size())
                            .withReplicationFactor(
                                (short) d.partitions().get(0).replicas().size())))
        .thenComposeAsync(
            spec ->
                spec.map(s -> properties(name, s, admin)).orElseGet(() -> completedFuture(null)))
        .thenApply(Optional::of)
        .exceptionally(e -> empty());
  }

  private static int intValue(final ConfigEntry entry) {
    return ofNullable(entry.value()).map(Integer::parseInt).orElse(-1);
  }

  private static CompletionStage<Map<String, Map<String, Long>>> messageLag(
      final String topic, final Admin admin) {
    return Kafka.messageLag(topic, admin)
        .thenApply(
            map ->
                map.entrySet().stream()
                    .collect(toMap(Entry::getKey, e -> partitionsAsStrings(e.getValue()))));
  }

  private static String name(final KafkaTopic resource) {
    return ofNullable(resource.getSpec().name).orElseGet(() -> resource.getMetadata().getName());
  }

  private static Map<String, Long> partitionsAsStrings(final Map<TopicPartition, Long> lags) {
    return lags.entrySet().stream()
        .collect(toMap(e -> valueOf(e.getKey().partition()), Entry::getValue));
  }

  private static CompletionStage<KafkaTopicSpec> properties(
      final String name, final KafkaTopicSpec spec, final Admin admin) {
    final ConfigResource key = new ConfigResource(TOPIC, name);

    return admin
        .describeConfigs(set(key))
        .all()
        .thenApply(map -> map.get(key))
        .thenApply(Config::entries)
        .thenApply(entries -> setProperties(spec, entries))
        .toCompletionStage();
  }

  private static Map<String, String> properties(final KafkaTopicSpec spec) {
    return map(
        concat(
            spec.maxMessageBytes != -1
                ? Stream.of(pair(MAX_MESSAGE_BYTES, valueOf(spec.maxMessageBytes)))
                : Stream.empty(),
            spec.retentionBytes != -1
                ? Stream.of(pair(RETENTION_BYTES, valueOf(spec.retentionBytes)))
                : Stream.empty(),
            spec.retentionMilliseconds != -1
                ? Stream.of(pair(RETENTION_MS, valueOf(spec.retentionMilliseconds)))
                : Stream.empty()));
  }

  private static CompletionStage<KafkaTopicSpec> reconcile(
      final String name,
      final KafkaTopicSpec spec,
      final Admin admin,
      final Map<String, Object> config) {
    return getTopic(name, admin)
        .thenComposeAsync(
            topic ->
                topic
                    .map(
                        t ->
                            anyChanged(t, spec)
                                ? updateTopic(name, spec, admin)
                                : completedFuture(t))
                    .orElseGet(() -> createTopic(name, spec, admin, config)));
  }

  private static KafkaTopicSpec setProperties(
      final KafkaTopicSpec spec, final Collection<ConfigEntry> entries) {
    return entries.stream().reduce(spec, KafkaTopicReconciler::setProperty, (s1, s2) -> s1);
  }

  private static KafkaTopicSpec setProperty(final KafkaTopicSpec spec, final ConfigEntry entry) {
    switch (entry.name()) {
      case MAX_MESSAGE_BYTES:
        return spec.withMaxMessageBytes(intValue(entry));
      case RETENTION_BYTES:
        return spec.withRetentionBytes(intValue(entry));
      case RETENTION_MS:
        return spec.withRetentionMilliseconds(intValue(entry));
      default:
        return spec;
    }
  }

  private static KafkaTopicStatus status(final KafkaTopic resource) {
    return ofNullable(resource.getStatus()).orElseGet(KafkaTopicStatus::new);
  }

  private static CompletionStage<KafkaTopicSpec> updateTopic(
      final String name, final KafkaTopicSpec spec, final Admin admin) {
    LOGGER.info(() -> "Update topic " + name);

    return admin
        .incrementalAlterConfigs(map(pair(new ConfigResource(TOPIC, name), alterConfigs(spec))))
        .all()
        .toCompletionStage()
        .thenApply(r -> spec);
  }

  private UpdateControl<KafkaTopic> error(final KafkaTopic resource, final Throwable t) {
    LOGGER.log(SEVERE, t, t::getMessage);
    timerEventSource.scheduleOnce(resource, 5000);
    resource.setStatus(status(resource).withException(t));

    return patchStatus(resource);
  }

  public Map<String, EventSource> prepareEventSources(
      final EventSourceContext<KafkaTopic> context) {
    timerEventSource.start();

    return map(pair(generateNameFor(timerEventSource), timerEventSource));
  }

  public UpdateControl<KafkaTopic> reconcile(
      final KafkaTopic resource, final Context<KafkaTopic> context) {
    final Map<String, Object> config = getConfig();
    final String name = name(resource);

    return tryToGetWith(
            () -> create(config),
            admin ->
                reconcile(name, resource.getSpec(), admin, config)
                    .thenComposeAsync(spec -> messageLag(name, admin))
                    .thenApply(
                        messageLag -> {
                          timerEventSource.scheduleOnce(resource, 60000);
                          resource.setStatus(status(resource).withMessageLag(messageLag));

                          return patchStatus(resource);
                        })
                    .exceptionally(e -> error(resource, e))
                    .toCompletableFuture()
                    .join(),
            e -> error(resource, e))
        .orElse(null);
  }
}
