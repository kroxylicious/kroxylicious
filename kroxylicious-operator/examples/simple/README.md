These manifests declare a minimal proxy which simply forwards all requests to a single backend Apache Kafka cluster without doing anything clever either in terms of networking of protocol filters.

For the purposes of this example we use
* an in-cluster Apache Kafka provided by [Strimzi](https://strimzi.io/)

As such this example should work more-or-less any Kube cluster which is able to pull the necessary images.
But please note that Kroxylicious should work with any Kafka protocol compatible broker.

To try this example out:
1. Install kubectl
2. `cd` to this directory
3. `kubectl apply -f .`