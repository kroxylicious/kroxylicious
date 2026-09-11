These manifests declare a Kafka proxy which implements isolation of resources based on the User principal extracted from a SASL exchange

For the purposes of this example we use
* an in-cluster Kafka provided by [Strimzi](https://strimzi.io/). Installed using `kubectl create ns my-proxy && kubectl create -f 'https://strimzi.io/install/latest?namespace=my-proxy' -n my-proxy`.

To try this example out:
1. Install kubectl
2. `cd` to this directory
3. `kubectl apply -f .`
4. Producing to a topic:
   ```
   kubectl exec -it my-cluster-dual-role-0 -n my-proxy -- /bin/bash ./bin/kafka-console-producer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --topic foo --producer-property security.protocol=SASL_PLAINTEXT --producer-property sasl.mechanism=SCRAM-SHA-512 --producer-property sasl.jaas.config='org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="changeit";'
   ```
5. Now consume from that topic using a group `my-group` as user Alice:
   ```
   kubectl exec -it my-cluster-dual-role-0 -n my-proxy -- /bin/bash ./bin/kafka-console-consumer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --group my-group --topic foo --consumer-property security.protocol=SASL_PLAINTEXT --consumer-property sasl.mechanism=SCRAM-SHA-512 --consumer-property sasl.jaas.config='org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="changeit";' --from-beginning
   ```
6. Now consume from that topic using the same `my-group` group as user Bob.
   ```
   kubectl exec -it my-cluster-dual-role-0 -n my-proxy -- /bin/bash ./bin/kafka-console-consumer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --group my-group --topic foo --consumer-property security.protocol=SASL_PLAINTEXT --consumer-property sasl.mechanism=SCRAM-SHA-512 --consumer-property sasl.jaas.config='org.apache.kafka.common.security.scram.ScramLoginModule required username="bob" password="changeit";' --from-beginning
   ```
   Notice that you see the same messages printed for each consumer despite them having the same group id. This is because they have been isolated into distinct groups per user.
7. If you check what groups exist in the upstream broker you will see there are two groups, prefixed with the users name:
   ```
   kubectl exec -it my-cluster-dual-role-0 -n my-proxy -- /bin/bash ./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9093 --list
   ```