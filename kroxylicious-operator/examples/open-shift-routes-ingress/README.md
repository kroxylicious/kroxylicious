These manifests declare a minimal proxy which simply forwards all requests to a single backend Apache Kafka cluster without doing anything clever either in terms of networking of protocol filters.
In this example, the downstream connection (that is, between the Kafka client and the proxy) uses TLS.

For the purposes of this example we use
* an in-cluster Apache Kafka provided by [Strimzi](https://strimzi.io/)
* cert manager to create a server certificate which is signed by a self-signed issuer.

To try this example out:
1. Install oc
2. `cd` to this directory
3. Apply cert-manager
   ```shell
    oc apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.17.2/cert-manager.yaml
    oc wait deployment/cert-manager-webhook --for=condition=Available=True --timeout=300s -n cert-manager
    ```
4. Install Strimzi and a Kafka cluster
   ```shell
   oc create namespace kafka
   oc apply -n kafka -f 'https://strimzi.io/install/latest?namespace=kafka'
   oc wait -n kafka deployment/strimzi-cluster-operator --for=condition=Available=True --timeout=300s
   oc apply -n kafka -f https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml
   oc wait -n kafka kafka/my-cluster --for=condition=Ready --timeout=300s
   ```
4. Apply the example
   ```shell
   INGRESS_DOMAIN="$(oc get ingresses.config/cluster -o jsonpath={.spec.domain})"
   find . -type f -name '*.yaml' -exec sed -i "s/\\\$\\\$INGRESS_DOMAIN\\\$\\\$/${INGRESS_DOMAIN}/g" {} \;
   oc apply -f .
   ```
5. Wait for external ip to be allocated to service `my-cluster-sni`.
   ```shell
   oc get service -n  my-proxy simple-sni -o jsonpath='{.status.loadBalancer.ingress[0].ip}' --watch
   ```
6. Try producing and consuming some messages with commands like this (obtain a kafka distribution):
   ```
   CA=$(oc get secret -n my-proxy server-certificate -o json | jq -r ".data.\"ca.crt\" | @base64d")
   BOOTSTRAP="$(oc get vkc -n my-proxy my-cluster -ojsonpath='{.status.ingresses[].bootstrapServer}')"
   ./bin/kafka-console-producer.sh --bootstrap-server ${BOOTSTRAP} --topic mytopic --producer-property ssl.truststore.type=PEM --producer-property security.protocol=SSL --producer-property ssl.truststore.certificates="${CA}"
   ./bin/kafka-console-consumer.sh --bootstrap-server ${BOOTSTRAP} --topic mytopic --from-beginning --consumer-property ssl.truststore.type=PEM --consumer-property security.protocol=SSL --consumer-property ssl.truststore.certificates="${CA}"
   ```

