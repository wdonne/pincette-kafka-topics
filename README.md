# The Kafka Topics Operator

With this Kubernetes operator you can manage Kafka topics. The `KafkaTopic` custom resource describes a Kafka topic. It will create the topic if it doesn't exist and make sure the properties it supports are not changed in any other way. When it detects such a change it will put back the values in the custom resource. When such a resource is deleted, the Kafka topic will not be deleted. A resource looks like this:

```
apiVersion: pincette.net/v1
kind: KafkaTopic
metadata:
  name: test-topic
  namespace: test-kafka-topics
spec:
  maxMessageBytes: 1000000
  name: test-topic
  partitions: 1
  replicationFactor: 1
  retentionBytes: -1
  retentionMilliseconds: -1
status:
  phase: Ready
```

The only mandatory field is `name`. Defaults are defined at the level of the Kafka cluster.

The `status` field may also contain the field `messageLag`. It is updated every minute. The keys in the field are the Kafka consumer groups that consume the topic. The values are objects that contain the message lag for each topic partition if there is any.

```
apiVersion: pincette.net/v1
kind: KafkaTopic
metadata:
  name: test-topic
  namespace: test-kafka-topics
spec:
  maxMessageBytes: 1000000
  name: test-topic
  partitions: 1
  replicationFactor: 1
  retentionBytes: -1
  retentionMilliseconds: -1
status:
  messageLag:
    test-consumer-group:
      "0": 1
  phase: Ready  
```

Install the operator as follows:

```
kubectl apply -f https://github.com/wdonne/pincette-kafka-topics/raw/main/manifests/install.yaml
```

You need to provide a `ConfigMap` like this:

```
apiVersion: v1
kind: ConfigMap
metadata:
  namespace: kafka-topics
  name: config
data:
  application.conf: |
    bootstrap.servers = "localhost:9092"
```

The syntax is [Lightbend Config](https://github.com/lightbend/config). You may add other Kafka properties to it. If there would be secrets in those properties, then you should kustomize the `Deployment` resource to mount a secret instead. You could also mount both and use an `include` statement to include the secret in the config, for example.