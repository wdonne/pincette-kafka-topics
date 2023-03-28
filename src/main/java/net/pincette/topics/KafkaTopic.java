package net.pincette.topics;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;

@Group("pincette.net")
@Version("v1")
public class KafkaTopic extends CustomResource<KafkaTopicSpec, KafkaTopicStatus>
    implements Namespaced {}
