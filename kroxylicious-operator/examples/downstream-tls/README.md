These manifests declare a minimal proxy which simply forwards all requests to a single backend Apache Kafka cluster without doing anything clever either in terms of networking of protocol filters.
In this example, the downstream connection (that is, between the Kafka client and the proxy) uses TLS.

For the purposes of this example we use
* an in-cluster Apache Kafka provided by [Strimzi](https://strimzi.io/)
* cert manager to create a server certificate which is signed by a self-signed issuer.

To try this example out:
1. Install kubectl
2. `cd` to this directory
3. Apply cert-manager
   ```shell
    kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.17.2/cert-manager.yaml
    kubectl wait deployment/cert-manager-webhook --for=condition=Available=True --timeout=300s -n cert-manager
    ```
4. Apply the example
   ```shell
   kubectl apply -f .
   ```
5. Try producing and consuming some messages with commands like this:
   ```
   CA=$(kubectl get secret -n my-proxy server-certificate -o json | jq -r ".data.\"ca.crt\" | @base64d")
   kubectl exec -it my-cluster-dual-role-0 -n kafka -- /bin/bash ./bin/kafka-console-producer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --topic mytopic --producer-property ssl.truststore.type=PEM --producer-property security.protocol=SSL --producer-property ssl.truststore.certificates="${CA}"
   kubectl exec -it my-cluster-dual-role-0 -n kafka -- /bin/bash ./bin/kafka-console-consumer.sh --bootstrap-server my-cluster-cluster-ip-bootstrap.my-proxy.svc.cluster.local:9292 --topic mytopic --from-beginning --consumer-property ssl.truststore.type=PEM --consumer-property security.protocol=SSL --consumer-property ssl.truststore.certificates="${CA}"
   ```

