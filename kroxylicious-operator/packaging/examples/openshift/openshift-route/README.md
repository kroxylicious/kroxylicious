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
    kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.20.0/cert-manager.yaml
    kubectl wait deployment/cert-manager-webhook --for=condition=Available=True --timeout=300s -n cert-manager
    ```
4. Prepare the certificate
   ```shell
   DOMAIN=$(oc get ingresscontrollers.operator.openshift.io -n openshift-ingress-operator default  -o=jsonpath='{.spec.domain}')
   DOMAIN=${DOMAIN} yq e -i '.spec.dnsNames = ([ "my-cluster-bootstrap.", "my-cluster-0.", "my-cluster-1.", "my-cluster-2."] | map(. + env(DOMAIN)))' 06.Certificate.server-certificate.yaml
   ```
5. Apply the example
   ```shell
   kubectl apply -f .
   ```
6. Try producing and consuming some messages with commands like this:
   ```
   CA=$(kubectl get secret -n my-proxy server-certificate -o json | jq -r ".data.\"ca.crt\" | @base64d")
   kubectl exec -it my-cluster-dual-role-0 -n kafka -- /bin/bash -c "echo '${CA}' > /tmp/ca.pem; ./bin/kafka-console-producer.sh --bootstrap-server my-cluster-bootstrap.${DOMAIN}:443 --topic mytopic --command-property ssl.truststore.type=PEM --command-property security.protocol=SSL --command-property ssl.truststore.location=/tmp/ca.pem"
   kubectl exec -it my-cluster-dual-role-0 -n kafka -- /bin/bash -c "echo '${CA}' > /tmp/ca.pem; ./bin/kafka-console-consumer.sh --bootstrap-server my-cluster-bootstrap.${DOMAIN}:443 --topic mytopic --from-beginning --command-property ssl.truststore.type=PEM --command-property security.protocol=SSL --command-property ssl.truststore.location=/tmp/ca.pem"
   ```

