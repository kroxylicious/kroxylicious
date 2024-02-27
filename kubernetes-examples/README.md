# Kubernetes Examples

This directory contains a number of example that demonstrate different features of Kroxylicious
in a Kubernetes environment.

The instructions for running the examples are given below.


# Example Catalogue

| example                                                | showcases                                                                              |
|--------------------------------------------------------|----------------------------------------------------------------------------------------|
| [record-encryption](record-encryption/README.md)       | kroxylicious providing encryption-at-rest.                                             |
| [multi-tenant](./multi-tenant/README.md)               | kroxylicious providing multi-tenancy - present one kafka cluster as if it were many.   |
| [portperbroker_plain](./portperbroker_plain/README.md) | kroxylicious+strimzi using plain connections upstream/downstream.                      |
| [snirouting_tls](./snirouting_tls/README.md)           | kroxylicious+strimzi using TLS upstream/downstream connections with SNI based routing. |

## Prerequisites to run the kubernetes-examples

* [Minikube](https://minikube.sigs.k8s.io/docs/start)
* [kubectl](https://kubernetes.io/docs/tasks/tools)
* [kustomize](https://kubectl.docs.kubernetes.io/installation/kustomize/)
* [helm](https://helm.sh/docs/helm/helm_install/)
* [kaf](https://github.com/birdayz/kaf)
* Mac OS X users must have [`gsed`](https://formulae.brew.sh/formula/gnu-sed)

If you want build your own kroxylicious images you'll additionally need: 

* [Docker engine](https://docs.docker.com/engine/install) or [podman](https://podman.io/docs/installation) 
* Access to a container registry such as [quay.io](https://quay.io) or [docker.io](https://docker.io) with a public accessible repository within the registry named `kroxylicious`.


## Running the kubernetes-examples

Kroxylicious can be containerised and run on Minikube against a [Strimzi](https://strimzi.io) managed Kafka cluster.

To run using pre-built Kroxylcious images:

```shell
./scripts/run-example.sh ${kubernetes_example_directory}
```
where `${kubernetes_example_directory}` is replaced by a path to an example directory e.g. `./kubernetes-examples/portperbroker_plain`.

To use an alternative image for Kroxylicious, set the `KROXYLICIOUS_IMAGE` environment variable.

```shell
KROXYLICIOUS_IMAGE=quay.io/kroxylicious/kroxylicious:x.y.z ./scripts/run-example.sh ${kubernetes_example_directory}
```

This `run-example.sh` script does the following:
1. starts minikube (if necessary)
1. installs cert manager, vault and strimzi as necessary.
1. installs a Kafka cluster using Strimzi into minikube
1. installs kroxylicious into minikube, configured to proxy the cluster

