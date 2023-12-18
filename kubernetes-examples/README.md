# Kubernetes Examples

This directory contains a number of example that demonstrate different features of Kroxylicious
in a Kubernetes environment.

The instructions for running the examples are given below.


# Example Catalogue

| example                                                | showcases                                                                              |
|--------------------------------------------------------|----------------------------------------------------------------------------------------|
| [envelope-encryption](./envelope-encryption/README.md) | kroxylicious providing encryption-at-rest.                                             |
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

```shell
REGISTRY_DESTINATION=quay.io/$your_quay_org$/kroxylicious ./scripts/run-example.sh ${kubernetes_example_directory}
```
where `${kubernetes_example_directory}` is replaced by a path to an example directory e.g. `./kubernetes-examples/portperbroker_plain`.

This `run-example.sh` script does the following:
1. builds and pushes a kroxylicious image to specified container registry
2. starts minikube
3. installs cert manager, vault and strimzi
4. installs a 3-node Kafka cluster using Strimzi into minikube
5. installs kroxylicious into minikube, configured to proxy the cluster

> NOTE: If the kroxylicious pod doesn't come up, but it's stuck on ImagePullBackOff with "unauthorized: access to the requested resource is not authorized" error,
it could mean you have to make the Quay image as public.

If you want to only build and push an image to the container registry you can run `PUSH_IMAGE=y REGISTRY_DESTINATION=quay.io/$your_quay_org$/kroxylicious ./scripts/deploy-image.sh`

To change the container engine to podman set `CONTAINER_ENGINE=podman`

