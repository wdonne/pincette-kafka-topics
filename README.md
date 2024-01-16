# The Kafka Topics Operator

With this Kubernetes operator you can manage Kafka topics. The `KafkaTopic` custom resource describes a Kafka topic. It will create the topic if it doesn't exist and make sure the properties it supports are not changed in any other way. When it detects such a change it will put back the values in the custom resource. When such a resource is deleted, the Kafka topic will not be deleted. A resource looks like this:

```yaml
apiVersion: pincette.net/v1
kind: KafkaTopic
metadata:
  name: test-topic
  namespace: test-kafka-topics
spec:
  maxMessageBytes: 1000000
  partitions: 1
  replicationFactor: 1
```

The field `name` is optional. You can use it when the topic name doesn't comply with the restrictions for `metadata.name`. Defaults are defined at the level of the Kafka clusteri and with the configuration fields `defaultPartitions` and `defaultReplicationFactor`.

The `status` field may also contain the field `messageLag`. It is updated every minute. The keys in the field are the Kafka consumer groups that consume the topic. The values are objects that contain the message lag for each topic partition if there is any.

```yaml
apiVersion: pincette.net/v1
kind: KafkaTopic
metadata:
  name: test-topic
  namespace: test-kafka-topics
spec:
  maxMessageBytes: 1000000
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

```bash
helm repo add pincette https://pincette.net/charts
helm repo update
helm install kafka-topics pincette/kafka-topics --namespace kafka-topics --create-namespace
```

The default chart values expect you to provide a `Secret` in the `kafka-topics` namespace (or the one you have chosen) with the name `config` like this:

```yaml
apiVersion: v1
kind: Secret
metadata:
  namespace: kafka-topics
  name: config
stringData:
  application.conf: |
    bootstrap.servers = "localhost:9092"
    defaultPartitions = 4
    defaultReplicationFactor = 3    
```

The syntax is [Lightbend Config](https://github.com/lightbend/config). If your configuration has partly secret information and partly non-secret information, then you can load both a secret and a config map. Then you can include one in the other with a Lightbend include statement. The default command in the container image expects to find the result in `/conf/application.conf`, but you can change this in the values file. The fields `defaultPartitions` and `defaultReplicationFactor` are optional.