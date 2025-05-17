These manifests declare a Kafka proxy which does _record validation_. 

For the purposes of this example we use
* an in-cluster Kafka provided by [Strimzi](https://strimzi.io/)
* an in-cluster registry provided by [Apicurio Registry](https://www.apicur.io/registry/)

As such this example should work more-or-less any Kube cluster which is able to pull the necessary images.
But note that this should work with any API-compatible registry implementation, such as Confluent Registry. 

To try this example out:
1. Install kubectl
2. `cd` to this directory
3. `kubectl apply -f .`