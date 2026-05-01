These manifests declare a minimal proxy which forwards all requests to a single backend Apache Kafka cluster deployed by Strimzi.
The proxy automatically discovers the upstream bootstrap servers and TLS trust anchor (cluster CA certificate) via a reference to the Strimzi Kafka CR in the KafkaService CR.

This demonstrates the `strimziKafkaRef` feature which:
* Reads bootstrap servers from the Strimzi `Kafka` CR's `status.listeners`
* Automatically trusts the Strimzi cluster CA when `trustStrimziCaCertificate: true` is set (eliminating manual CA extraction)
* Falls back to explicit `tls.trustAnchorRef` if provided (explicit config always takes precedence)

For the purposes of this example we use:
* Strimzi Operator installed from OLM
* An in-cluster Kafka cluster with TLS-enabled listeners

**Security note:** Using `trustStrimziCaCertificate: true` delegates certificate trust to the Strimzi Operator. The Strimzi CA secret must be in the same namespace as the KafkaService.

To try this example out:
1. Install `kubectl`
2. If OLM is not present in your Kubernetes Cluster, install it by following https://olm.operatorframework.io/docs/getting-started/
   `operator-sdk olm install`
3. `cd` to this directory
4. Apply all manifests:
   `kubectl apply -f .`
5. Wait for the Kafka cluster to be ready:
   `kubectl wait kafka/my-cluster -n my-proxy --for=condition=Ready --timeout=300s`
6. Try producing and consuming some messages with commands like this:
   ```
   kubectl exec -it my-cluster-dual-role-0 -n my-proxy -- /bin/bash ./bin/kafka-console-producer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --topic mytopic
   kubectl exec -it my-cluster-dual-role-0 -n my-proxy -- /bin/bash ./bin/kafka-console-consumer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --topic mytopic --from-beginning
   ```

**Troubleshooting:** If the KafkaService shows `ResolvedRefs: False`, check:
* Strimzi Operator is installed: `kubectl get crd kafkas.kafka.strimzi.io`
* Kafka cluster exists and is ready: `kubectl get kafka my-cluster -n my-proxy`
* Strimzi CA secret exists: `kubectl get secret my-cluster-cluster-ca-cert -n my-proxy`
