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
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.map;
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
    return oldSpec.maxMessageBytes != newSpec.maxMessageBytes
        || oldSpec.retentionBytes != newSpec.retentionBytes
        || oldSpec.retentionMilliseconds != newSpec.retentionMilliseconds;
  }

  private static CompletionStage<KafkaTopicSpec> createTopic(
      final KafkaTopicSpec spec, final Admin admin) {
    LOGGER.info(() -> "Create topic " + spec.name);

    return admin
        .createTopics(
            set(
                new NewTopic(spec.name, spec.partitions, (short) spec.replicationFactor)
                    .configs(properties(spec))))
        .all()
        .toCompletionStage()
        .thenApply(r -> spec);
  }

  private static Map<String, Object> getConfig() {
    return adminConfig(fromConfig(defaultOverrides().withFallback(loadDefault())));
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
                            .withName(name)
                            .withPartitions(d.partitions().size())
                            .withReplicationFactor(
                                (short) d.partitions().get(0).replicas().size())))
        .thenComposeAsync(
            spec -> spec.map(s -> properties(s, admin)).orElseGet(() -> completedFuture(null)))
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

  private static Map<String, Long> partitionsAsStrings(final Map<TopicPartition, Long> lags) {
    return lags.entrySet().stream()
        .collect(toMap(e -> valueOf(e.getKey().partition()), Entry::getValue));
  }

  private static CompletionStage<KafkaTopicSpec> properties(
      final KafkaTopicSpec spec, final Admin admin) {
    final ConfigResource key = new ConfigResource(TOPIC, spec.name);

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
      final KafkaTopicSpec spec, final Admin admin) {
    return getTopic(spec.name, admin)
        .thenComposeAsync(
            topic ->
                topic
                    .map(t -> anyChanged(t, spec) ? updateTopic(spec, admin) : completedFuture(t))
                    .orElseGet(() -> createTopic(spec, admin)));
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

  private static CompletionStage<KafkaTopicSpec> updateTopic(
      final KafkaTopicSpec spec, final Admin admin) {
    LOGGER.info(() -> "Update topic " + spec.name);

    return admin
        .incrementalAlterConfigs(
            map(pair(new ConfigResource(TOPIC, spec.name), alterConfigs(spec))))
        .all()
        .toCompletionStage()
        .thenApply(r -> spec);
  }

  private UpdateControl<KafkaTopic> error(final KafkaTopic resource, final Throwable t) {
    LOGGER.log(SEVERE, t, t::getMessage);
    timerEventSource.scheduleOnce(resource, 5000);
    resource.setStatus(new KafkaTopicStatus(t.getMessage()));

    return patchStatus(resource);
  }

  public Map<String, EventSource> prepareEventSources(
      final EventSourceContext<KafkaTopic> context) {
    timerEventSource.start();

    return map(pair(generateNameFor(timerEventSource), timerEventSource));
  }

  public UpdateControl<KafkaTopic> reconcile(
      final KafkaTopic resource, final Context<KafkaTopic> context) {
    return tryToGetWith(
            () -> create(getConfig()),
            admin ->
                reconcile(resource.getSpec(), admin)
                    .thenComposeAsync(spec -> messageLag(spec.name, admin))
                    .thenApply(
                        messageLag -> {
                          timerEventSource.scheduleOnce(resource, 60000);
                          resource.setStatus(new KafkaTopicStatus(messageLag));

                          return patchStatus(resource);
                        })
                    .exceptionally(e -> error(resource, e))
                    .toCompletableFuture()
                    .join(),
            e -> error(resource, e))
        .orElse(null);
  }
}