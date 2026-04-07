These manifests declare a minimal proxy which simply forwards all requests to a single backend Apache Kafka cluster which has been deployed by Strimzi.
The proxy discovers the upstream bootstrap and trust anchors via a reference to the Strimzi Kafka CR in the KafkaService CR.

Strimzi is installed from OLM.

To try this example out:
1. Install `kubectl`
2. If OLM is not present in your Kubernetes Cluster, install it by following https://olm.operatorframework.io/docs/getting-started/
   `operator-sdk olm install`
3. `cd` to this directory
4. Apply the proxy example and stand up a Kafka cluster in the same namespace.
   `kubectl apply -f .`
5. Try producing and consuming some messages with commands like this:
   ```
   kubectl exec -it my-cluster-dual-role-0 -n my-proxy -- /bin/bash ./bin/kafka-console-producer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --topic mytopic
   kubectl exec -it my-cluster-dual-role-0 -n my-proxy -- /bin/bash ./bin/kafka-console-consumer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --topic mytopic --from-beginning
   ```
