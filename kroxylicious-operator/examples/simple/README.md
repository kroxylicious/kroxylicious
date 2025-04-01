These manifests declare a minimal proxy which simply forwards all requests to a single backend Apache Kafka cluster without doing anything clever either in terms of networking of protocol filters.

For the purposes of this example we use
* an in-cluster Apache Kafka provided by [Strimzi](https://strimzi.io/)

As such this example should work more-or-less any Kube cluster which is able to pull the necessary images.
But please note that Kroxylicious should work with any Kafka protocol compatible broker.

To try this example out:
1. Install kubectl
2. `cd` to this directory
3. `kubectl apply -f .`
4. Try producing and consuming some messages with commands like this:
   ```
   kubectl exec -it my-cluster-dual-role-0 -n kafka -- /bin/bash ./bin/kafka-console-producer.sh --bootstrap-server my-cluster-cluster-ip.my-proxy.svc.cluster.local:9292 --topic mytopic
   kubectl exec -it my-cluster-dual-role-0 -n kafka -- /bin/bash ./bin/kafka-console-consumer.sh --bootstrap-server my-cluster-cluster-ip.my-proxy.svc.cluster.local:9292 --topic mytopic --from-beginning
   ```
