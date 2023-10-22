# Development Guide for Kroxylicious

This document gives a detailed breakdown of the various build processes and options for building the Kroxylicious from source.

<!-- TOC -->
* [Development Guide for Kroxylicious](#development-guide-for-kroxylicious)
  * [Build status](#build-status)
  * [Build Prerequisites](#build-prerequisites)
  * [Prerequisites to run the kubernetes-examples](#prerequisites-to-run-the-kubernetes-examples)
  * [Build](#build)
    * [Formatting the Code](#formatting-the-code)
  * [Run](#run)
    * [Debugging](#debugging)
  * [Running the kubernetes-examples](#running-the-kubernetes-examples)
  * [IDE setup](#ide-setup)
    * [Intellij](#intellij)
  * [Setting Up in Windows Using WSL](#setting-up-in-windows-using-wsl)
    * [Installing WSL](#installing-wsl)
    * [Ensure appropriate tooling available](#ensure-appropriate-tooling-available)
  * [Running Integration Tests on Podman](#running-integration-tests-on-podman)
    * [DOCKER_HOST environment variable](#dockerhost-environment-variable)
    * [Podman/Testcontainers incompatibility](#podmantestcontainers-incompatibility)
    * [MacOS X](#macos-x)
    * [Linux](#linux)
    * [Verify that the fix is effective](#verify-that-the-fix-is-effective)
  * [Rendering documentation](#rendering-documentation)
  * [Using the GitHub CI workflows against a fork](#using-the-github-ci-workflows-against-a-fork)
<!-- TOC -->

## Build status
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=kroxylicious_kroxylicious&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=kroxylicious_kroxylicious) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=kroxylicious_kroxylicious&metric=coverage)](https://sonarcloud.io/summary/new_code?id=kroxylicious_kroxylicious)

## Build Prerequisites

- [JDK](https://openjdk.org/projects/jdk/17/) (version 17 and above) - JDK
- [`mvn`](https://maven.apache.org/index.html) (version 3.5 and above) - Maven CLI
- [`docker`](https://docs.docker.com/install/) or [`podman`](https://podman.io/docs/installation) - Docker or Podman

> :warning: **If you are using Podman please see [these notes](#running-integration-tests-on-podman) below**

## Prerequisites to run the kubernetes-examples

* User must have access to a container registry such as [quay.io](https://quay.io) or [docker.io](https://docker.io).
  Create a public accessible repository within the registry named `kroxylicious`.
* Minikube [installed](https://minikube.sigs.k8s.io/docs/start)
* kubectl [installed](https://kubernetes.io/docs/tasks/tools)
* kustomize [installed](https://kubectl.docs.kubernetes.io/installation/kustomize/)
* OSX users must have `gsed` [installed](https://formulae.brew.sh/formula/gnu-sed)
* Docker engine [installed](https://docs.docker.com/engine/install) or [podman](https://podman.io/docs/installation) 


## Build

JDK version 21 or newer, and Apache Maven are required for building this project.

Kroxylicious targets language level 17, except for the `integrationtests` module
which targets 21 to access some new language features.

Build the project like this:

```shell
mvn clean install
```

The running of the tests can be controlled with the following Maven properties:

| property           | description                                                                               |
|--------------------|-------------------------------------------------------------------------------------------|
| `-DskipUTs=true`   | skip unit tests                                                                           |
| `-DskipITs=true`   | skip integration tests                                                                    |
| `-DskipTests=true` | skip all tests                                                                            |
| `-Pdebug`          | enables logging so you can see what the Kafka clients, Proxy and in VM brokers are up to. |

The kafka environment used by the integrations tests can be _defaulted_ with these two environment variables.

| env var                       | default | description                                                                                                                             |
|-------------------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `TEST_CLUSTER_EXECUTION_MODE` | `IN_VM` | `IN_VM` or `CONTAINER`. if `IN_VM`, kafka will be run same virtual machines as the integration test. Otherwise containers will be used. |
| `TEST_CLUSTER_KRAFT_MODE`     | `true`  | if true, kafka will be run in kraft mode.                                                                                               |

When the integration-tests are run in `CONTAINER` mode, the kafka/zookeeper logs are written to a location specified by
the `container.logs.dir`  system property. When run through Maven this is defaulted to `integrationtests/target/container-logs`.

Pass the `-Dquick` option to skip all tests and non-essential plug-ins and create the output artifact as quickly as possible:

```shell
mvn clean verify -Dquick
```

Run the following command to format the source code and organize the imports as per the project's conventions:

```shell
mvn process-sources
```

Build with the `dist` profile to create distribution artefacts (see [kroxylicious-app](kroxylicious-app)):

```shell
mvn clean verify -Pdist -Dquick
```

The project provides some Kroxylicious-maintained Filter implementations that are not included in the distribution
by default. To also include the additional filters in the distribution (located under [kroxylicious-additional-filters](./kroxylicious-additional-filters)), 
activate the `withAdditionalFilters` profile:

```shell
mvn clean install -Pdist -Dquick -PwithAdditionalFilters
```

Run the following to add missing license headers e.g. when adding new source files:

```shell
mvn org.commonjava.maven.plugins:directory-maven-plugin:highest-basedir@resolve-rootdir license:format
```

### Formatting the Code
No one likes to argue about code formatting in pull requests, as project we take the stance that if we can't automate the formatting we are not going to argue about it either. Having said that we don't want a mishmash of conflicting styles! So we attack this from multiple angles.

1. Shared Code formatter settings. Included in the repo are code formatter settings for `Eclipse`, `InjtellJ` and `.editorconfig`.
2. The Continuous Integration (CI) job building Pull Requests will fail if there is formatting which doesn't pass our agreed conventions
3. We apply [Checkstyle](https://checkstyle.org/) validation to the project as well. You can find our [agreed ruleset](etc/checkstyle-custom_checks.xml) in the `etc` folder. We bind checkstyle to the `verify` phase of the build so `mvn clean verify` will validate the code is acceptable. 
4. We also employ [impsort-maven-plugin](https://code.revelc.net/impsort-maven-plugin/) to keep import order consistent which will re-order imports as part of the maven build.
5. We also have [formatter-maven-plugin](https://code.revelc.net/formatter-maven-plugin/) which will apply the project code style rules, this is driven from the Eclipse code formatter, as part of the maven build cycle.

## Run

Build with the `dist` profile as shown above, then execute this:

```shell
kroxylicious-app/target/kroxylicious-app-*-bin/kroxylicious-app-*/bin/kroxylicious-start.sh --config ${path_to_kroxylicious_config}
```

Or, to run with your own class path, run this instead:

```shell
KROXYLICIOUS_CLASSPATH="${additional_classpath_entries}" kroxylicious-app/target/kroxylicious-app-*-bin/kroxylicious-app-*/bin/kroxylicious-start.sh --config ${path_to_kroxylicious_config}
```

for example:

```shell
KROXYLICIOUS_CLASSPATH="/path/to/any.jar:/path/to/libs/dir/*" kroxylicious-app/target/kroxylicious-app-*-bin/kroxylicious-app-*/bin/kroxylicious-start.sh --config kroxylicious-app/example-proxy-config.yml
```

### Debugging
Logging is turned off by default for better performance. In case you want to debug, logging should be turned on in the `example-proxy-config.yml` file:
```yaml
  logNetwork: true
  logFrames: true
```

## Running the kubernetes-examples

Kroxylicious can be containerised and run on Minikube against a [Strimzi](https://strimzi.io) managed Kafka cluster.

Running:

```shell
minikube delete && REGISTRY_DESTINATION=quay.io/$your_quay_org$/kroxylicious ./scripts/run-with-strimzi.sh ${kubernetes_example_directory}
```
where `${kubernetes_example_directory}` is replaced by a path to an example directory e.g. `./kubernetes-examples/portperbroker_plain`.

This `run-with-strimzi.sh` script does the following:
1. builds and pushes a kroxylicious image to specified container registry
2. starts minikube
3. installs cert manager and strimzi
4. installs a 3-node Kafka cluster using Strimzi into minikube
5. installs kroxylicious into minikube, configured to proxy the cluster

> NOTE: If the kroxylicious pod doesn't come up, but it's stuck on ImagePullBackOff with "unauthorized: access to the requested resource is not authorized" error, 
it could mean you have to make the Quay image as public.

If you want to only build and push an image to the container registry you can run `PUSH_IMAGE=y REGISTRY_DESTINATION=quay.io/$your_quay_org$/kroxylicious ./scripts/deploy-image.sh`

To change the container engine to podman set `CONTAINER_ENGINE=podman`

## IDE setup

### Intellij

The project requires JDK-21 to build and run the `integrationtests` module and the IDEA project is configured to build against an SDK
named `temurin-21`. A suggested way to install this is with [skdman](https://sdkman.io/) using `sdk install java 21-tem`.

Run `mvn clean install -DskipTests` to install the project into your local maven repository (in `~/.m2`). This is necessary because
IDEA fails to synchronise the project if the kroxylicious maven plugin isn't available to maven.

Open the root `pom.xml` as a project.

Then navigate to `File > Project Structure > Project Settings` and update `SDK` to point at your install JDK 21 (it should be populated
as a suggestion if you used sdkman to install it).

In the IDEA Maven dialogue click on `Generate Sources and Update Folders For All Projects`.

Build the entire project by running `Build > Build Project` and then check that you can run `io.kroxylicious.proxy.FilterIT`

If you encounter any further issues with generated sources, you can try running `mvn clean install -DskipTests` again or running 
`Generate Sources and Update Folders` for the specific module that is having problems.

## Setting Up in Windows Using WSL
While Kroxylicious is a java application we've had reports of issues running the build natively on Windows and thus suggest using the Windows Subsystem for Linux (WSL) for development.
### Installing WSL

1. Enable the Windows Subsystem for Linux feature: To enable WSL, you need to enable the Windows Subsystem for Linux
   feature in the Windows Features dialog.
2. Install a Linux distribution from the Microsoft Store. The rest of these instructions assume a distribution (such as
   Ubuntu) which uses `apt` for package management, but the process should be similar for distributions using other
   package managers/ such as Fedora/`dnf`.
3. Launch the Linux distribution and Configure the Linux environment: After launching the Linux distribution, you can
   configure the environment by creating a user account and setting the password.
   With these steps, you should now have WSL installed and configured on your Windows system.
   For further assistance please see the [Microsoft documentation](https://learn.microsoft.com/en-us/windows/wsl/install)

### Ensure appropriate tooling available
1. Open the WSL window.
2. Update the packages using
    ```shell
    sudo apt update
    sudo apt upgrade
    ```
3. 
    1. Check the Java version by typing
      ```shell
      java --version
      ```
      Expect output similar to: 
      ```shell
      > java --version
   openjdk 19.0.2 2023-01-17
   OpenJDK Runtime Environment Temurin-19.0.2+7 (build 19.0.2+7)
   OpenJDK 64-Bit Server VM Temurin-19.0.2+7 (build 19.0.2+7, mixed mode, sharing)
   ```
    2. Update if needed: sample update command like:
    ```shell
    sudo apt update
    sudo apt upgrade
    sudo apt install openjdk-18-jre-headless
    ```
4. Ensure GIT is available
   1. ```shell
      git --version
      ```
      Expect a version string similar to `git version 2.37.1 (Apple Git-137.1)`
   2. Follow the [WSL-git tutorial](https://learn.microsoft.com/en-us/windows/wsl/tutorials/wsl-git) if needed.
5. Checkout Kroxylicious to `${kroxylicious-checkout}`
6. Build & develop following the [standard build](#Build) instructions

## Running Integration Tests on Podman

### DOCKER_HOST environment variable

On Linux, it may be necessary to configure the `DOCKER_HOST` environment variable to allow the tests to correctly use test containers.

```shell
DOCKER_HOST=unix://$(podman info --format '{{.Host.RemoteSocket.Path}}')
export DOCKER_HOST
```

### Podman/Testcontainers incompatibility

There is an incompatibility between HTTP connection timeout expectations of 
[testcontainers-java](https://github.com/testcontainers/testcontainers-java) and the Podman API. This
can result in sporadic test failures when running the Integration Tests under Podman.  It manifests as
failed or hanging REST API calls that leads to test failures and test hangs.

It affects Linux and Mac OS X.
On Linux it manifests as Http calls failing with a `Broken Pipe` exception. 
Similarly on MacOS we see a `localhost:XXX failed to respond`.

To workaround around the issue, tune the `service_timeout` so that the timeout is in sympathy with the
expectations of the underlying HttpClient defaults.

Do so by following these instructions.

### MacOS X

Start the `podman` machine as normal, then:

```shell
echo 'mkdir -p /etc/containers/containers.conf.d && printf "[engine]\nservice_timeout=91\n" > /etc/containers/containers.conf.d/service-timeout.conf' && systemctl restart podman.socket |  podman machine ssh --username root --
```

### Linux

As a privileged user:

```shell
mkdir -p /etc/containers/containers.conf.d && printf "[engine]\nservice_timeout=91\n" > /etc/containers/containers.conf.d/service-timeout.conf && systemctl restart podman.socket
```

### Verify that the fix is effective

On Linux, start this command:
```shell
socat - UNIX-CONNECT:$(podman info --format '{{.Host.RemoteSocket.Path}}')
```
On Mac OSX, Start this command:
```shell
time socat - UNIX-CONNECT:/var/run/docker.sock
```
the send this input (including the empty line):
```
GET /version HTTP/1.1
Host: www.example.com

```

You'll see an API response.  If the service_timeout change is effective, the socat
will continue for 3 minutes.  If `socat` terminates after about 10 seconds, the workaround
has been applied ineffectively.


## Rendering documentation

The `docs` directory has some user documentation written in [AsciiDoc](https://docs.asciidoctor.org/asciidoc/latest/) format.
You can render it to HTML using:

```shell
mvn org.asciidoctor:asciidoctor-maven-plugin:process-asciidoc@convert-to-html
```

The output will be in `target/html/master.html`. 

## Using the GitHub CI workflows against a fork

All CI [workflows](.github/workflows) defined by the project are expected to execute within the context of a fork, apart from [docker workflow](.github/workflows/docker.yml).
To enable the docker workflow, you need to configure three repository [variables](https://docs.github.com/en/actions/learn-github-actions/variables)
and one repository [secret](https://docs.github.com/en/actions/security-guides/using-secrets-in-github-actions).

* `REGISTRY_SERVER` variable - the server of the container registry service e.g. `quay.io` or `docker.io`
* `REGISTRY_USERNAME` variable - your username on the service (or username of your robot account)
* `REGISTRY_DESTINATION` variable - the push destination (without tag portion) e.g. `quay.io/<my org>/kroxylicious-developer`

* `REGISTRY_TOKEN` secret - the access token that corresponds to `REGISTRY_USERNAME` 

The workflow will push the container image to `${REGISTRY_DESTINATION}` so ensure that the `${REGISTRY_USERNAME}` user has sufficient write privileges. 
