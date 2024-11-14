This is the Kroxylicious operator for Kubernetes.

# Hacking and Debugging

If you want to iterate quickly on the operator the simplest way is to run it as a process on your host (i.e. *not* running it within a Kubernetes cluster).

Note: The Integration Tests will only run if your kubectl context is pointing at a cluster. For development, we recommend using  `minikube`, for example:

```bash
minikube start --kubernetes-version=latest --driver=podman 
````

You should now be able to run the tests using `mvn`.

If you want to run the `OperatorMain` (e.g. from your IDE, maybe for dubugging) then you'll need to install the CRD:

```bash
kubectl apply -f src/main/resources/META-INF/fabric8
```

You should now be able to play around with `KafkaProxy` CRs; read the "Creating a `KafkaProxy`" section.

Alternatively you can build the operator properly and run it within Kube...

# Building & installing the operator

## Building

Note: The Integration Tests will only run if your kubectl context is pointing at a cluster. For development, we recommend using  `minikube`, for example:

```bash
minikube start --kubernetes-version=latest --driver=podman 
````

Building the operator distribution

```bash
mvn package
````

should produce a `target/kroxylicious-operator-0.9.0-SNAPSHOT-bin` directory.

Build the operator image. For development purposes you can use the minikube registry directly using `minikube image build`, which will be faster than alternatives like pushing to quay.io from your host only for kube to pull the same image right back when you deploy the operator.

```bash
minikube image build . -t quay.io/kroxylicious/operator:latest \
--build-opt=build-arg=KROXYLICIOUS_VERSION=0.9.0-SNAPSHOT
```

## Installing the operator

```bash
kubectl apply -f install 
```

You can check that worked with something like

```bash
kubectl logs -n kroxylicious-operator pods/kroxylicious-operator-7cd88454c8-fjcxm operator
```

(your pod hash suffix will differ)

# Creating a `KafkaProxy`

```
kubectl apply -f examples/simple/
```

You can check that worked with something like

```
kubectl logs -n my-proxy pods/simple-647d99d9b5-hkwt2 proxy 
```

(your pod hash suffix will differ)

Note that on its own the proxy won't try to connect to the Kafka cluster.


# Testing

To test things properly you'll need to point your virtual clusters at a running Kafka and also run a Kafka client so the proxy is handling some load.