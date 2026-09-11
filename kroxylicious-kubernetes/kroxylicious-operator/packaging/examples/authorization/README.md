These manifests declare a Kafka proxy which implements authorization based on the authorizedId extracted from a SASL exchange

For the purposes of this example we use
* an in-cluster Kafka provided by [Strimzi](https://strimzi.io/). Installed using `kubectl create ns my-proxy && kubectl create -f 'https://strimzi.io/install/latest?namespace=my-proxy' -n my-proxy`.

As such this example should work more-or-less any Kube cluster which is able to pull the necessary images. But note that Kroxylicious also supports other KMSes.

To try this example out:
1. Install kubectl
2. `cd` to this directory
3. `kubectl apply -f .`
4. Try producing and consuming some messages with commands like this:
   ```
   kubectl exec -it my-cluster-dual-role-0 -n my-proxy -- /bin/bash ./bin/kafka-console-producer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --topic foo --producer-property security.protocol=SASL_PLAINTEXT --producer-property sasl.mechanism=SCRAM-SHA-512 --producer-property sasl.jaas.config='org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="changeit";'
   kubectl exec -it my-cluster-dual-role-0 -n my-proxy -- /bin/bash ./bin/kafka-console-consumer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --topic foo --consumer-property security.protocol=SASL_PLAINTEXT --consumer-property sasl.mechanism=SCRAM-SHA-512 --consumer-property sasl.jaas.config='org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="changeit";' --from-beginning
   ```
5. Now try producing to the denied topic foobar:
   ```
   kubectl exec -it my-cluster-dual-role-0 -n my-proxy -- /bin/bash ./bin/kafka-console-producer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --topic foobar --producer-property security.protocol=SASL_PLAINTEXT --producer-property sasl.mechanism=SCRAM-SHA-512 --producer-property sasl.jaas.config='org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="changeit";'
   ```
6. Now try producing to foobar as User bob, who is allowed access
   ```
   kubectl exec -it my-cluster-dual-role-0 -n my-proxy -- /bin/bash ./bin/kafka-console-producer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --topic foobar --producer-property security.protocol=SASL_PLAINTEXT --producer-property sasl.mechanism=SCRAM-SHA-512 --producer-property sasl.jaas.config='org.apache.kafka.common.security.scram.ScramLoginModule required username="bob" password="changeit";'
   kubectl exec -it my-cluster-dual-role-0 -n my-proxy -- /bin/bash ./bin/kafka-console-consumer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --topic foobar --consumer-property security.protocol=SASL_PLAINTEXT --consumer-property sasl.mechanism=SCRAM-SHA-512 --consumer-property sasl.jaas.config='org.apache.kafka.common.security.scram.ScramLoginModule required username="bob" password="changeit";' --from-beginning
   ```
7. Now try producing to foo as User bob, who is denied access
   ```
   kubectl exec -it my-cluster-dual-role-0 -n my-proxy -- /bin/bash ./bin/kafka-console-producer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --topic foo --producer-property security.protocol=SASL_PLAINTEXT --producer-property sasl.mechanism=SCRAM-SHA-512 --producer-property sasl.jaas.config='org.apache.kafka.common.security.scram.ScramLoginModule required username="bob" password="changeit";'
   ```