These manifests declare a minimal proxy which simply forwards all requests to a single backend Apache Kafka cluster without doing anything clever either in terms of networking of protocol filters.
In this example, the upstream connection to Kafka cluster uses TLS.

For the purposes of this example we use
* an in-cluster Apache Kafka provided by [Strimzi](https://strimzi.io/)

To try this example out:
1. Install kubectl
2. `cd` to this directory
3. `kubectl apply -f .`
4. ` kubectl create cm my-cluster-clients-ca-cert -n my-proxy --from-literal=ca.pem="$(oc get kafka --namespace kafka my-cluster -o json | jq -r '.status.listeners[1].certificates[0]')"`
5. Try producing and consuming some messages with commands like this:
   ```
   kubectl exec -it my-cluster-dual-role-0 -n kafka -- /bin/bash ./bin/kafka-console-producer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --topic mytopic
   kubectl exec -it my-cluster-dual-role-0 -n kafka -- /bin/bash ./bin/kafka-console-consumer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --topic mytopic --from-beginning
   ```
