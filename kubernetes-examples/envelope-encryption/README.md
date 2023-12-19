# Envelope Encryption Example on Kubernetes.

This example will demonstrate the `EnvelopeEncryption` filter which provides an Encryption-at-Rest solution for the
Apache Kafka(tm) which is transparent to both clients and brokers.

The `EnvelopeEncryption` filter works by intercepting all produce requests from applications and applies encryption to
the kafka records as they pass through Kroxylicious. On the consume path, the reverse happens - the filter intercepts the fetch responses and decrypts the records before they are sent to the application.


## What will the example demonstrate?

It will:

* deploy an Apache Kafka Cluster to Kubernetes using Strimzi.
* deploy an instance of Kroxylicious to the same Kubernetes cluster  to proxy the Kafka Cluster with configuration for
  `EnvelopeEncryption`.
* deploy an instance of HashiCorp Vault to store the keys used to encrypt the kafka records.
* use an off-cluster kafka producer and kafka consumer send and receive messages, and demonstrate
  that the records are indeed encrypted on the server.

Running the example will take about 10 minutes.

## Prerequisites

See [prerequistes](../README.md#prerequisites-to-run-the-kubernetes-examples).

## Running the example

1. Clone Kroxylicious Repository
    ```shell { adjunct="# We're going to demonstrate the Envelope Encryption feature of Kroxylicious. Let's start by cloning the repo." }
    git clone https://github.com/kroxylicious/kroxylicious.git
    ```
1. Change directory to it. 
    ```shell
    cd kroxylicious
    ```
1. Run the following script.
    ```shell { adjunct="# Now let's bring up minikube and install strimzi and vault. The script will also create a kafka cluster and deploy kroxylicious." }
    KROXYLICIOUS_IMAGE=quay.io/kroxylicious/kroxylicious-developer:0.4.0 ./scripts/run-example.sh kubernetes-examples/envelope-encryption
    ```

The script will first bring up Minikube. It will then deploy Strimzi and HashiCorp Vault.

It will then deploy a kafka cluster within namespace `kafka` called `my-cluster`.

It will deploy an instance of Kroxylicious within namespace `kroxylicious`. 

Kroxylicious is configured  to present a virtual cluster `my-cluster-proxy` using a bootstrap address of `minikube`
which will _target_ `my-cluster` i.e. the real kafka cluster or the cluster being proxied.

Kafka Clients connect to the virtual cluster and Kroxylicious proxies the traffic to the target cluster.
A Kubernetes `nodePort` service is used to expose the ports bound by Kroxylicious to your host.

It will configure Kroxylicious with the `EnvelopeEncryption` filter and wire it to the Vault instance. The
EnvelopeEncryption's key selector is configured to expect to find a key in the Vault instance named exactly
as the topic name.

4. Now we create an encryption keys in Vault for the topic we will use: `trades`.  You can do this using the Vault
   UI or with a command line like this:
    ```shell { adjunct="# Now let's create an encryption key within vault.  This'll be used to encypt messages sent to the topic of the same name." }
    kubectl exec -n vault vault-0 -- vault write -f transit/keys/trades
    ```
4. Finally, let's create the topic itself.
    ```shell { adjunct="# and finally let's create the topic itself." }
    kaf -b minikube:30192 topic create trades
    ```

That's the set-up all over. Now let's publish and consume some messages, and confirm they are indeed encrypted
on the cluster.

6. Publish a record via the proxy.
    ```shell { adjunct="# Great, so let's produce/consume via the proxy demonstrating the fact that encryption is transparent to the kafka application." }
   echo "ibm: 99" | kaf -b minikube:30192 produce trades
   ```
6. Consume a record via the proxy.
   ```shell { adjunct="# that's the record produced, now let's consume it." }
   kaf -b minikube:30192 consume trades
   ```
6. Now to verify that the record is truly encrypted on the Kafka Cluster, let's consume the message directly
   from the Kafka Cluster.
   ```shell { adjunct="# Finally, let's consume from the topic *direct from the cluster* showing the record is truly encrypted." }
   kubectl -n kafka run consumer -ti --image=quay.io/kroxylicious/kaf --rm=true --restart=Never -- kaf consume trades -b my-cluster-kafka-bootstrap:9092
   ```
