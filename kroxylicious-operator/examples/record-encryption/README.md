These manifests declare a Kafka proxy which does record encryption.

For the purposes of this example we use
* an in-cluster Kafka provided by [Strimzi](https://strimzi.io/)
* an in-cluster KMS provided by Hashicorp Vault

As such this example should work more-or-less any Kube cluster which is able to pull the necessary images. But note that Strimzi also supports other KMSes.

To try this example out:
1. Install kubectl
2. `cd` to this directory
3. `kubectl apply -f .`