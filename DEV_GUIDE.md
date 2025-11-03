# Development Guide for Kroxylicious

This document gives a detailed breakdown of the various build processes and options for building the Kroxylicious from source.

<!-- TOC -->
* [Development Guide for Kroxylicious](#development-guide-for-kroxylicious)
  * [Build status](#build-status)
  * [Build Prerequisites](#build-prerequisites)
  * [Build](#build)
    * [Formatting the Code](#formatting-the-code)
    * [Logging Conventions](#logging-conventions)
  * [Run](#run)
    * [Debugging](#debugging)
  * [Building and pushing Kroxylicious Container Images](#building-and-pushing-kroxylicious-container-images)
  * [IDE setup](#ide-setup)
    * [Intellij](#intellij)
  * [Setting Up in Windows Using WSL](#setting-up-in-windows-using-wsl)
    * [Installing WSL](#installing-wsl)
    * [Ensure appropriate tooling available](#ensure-appropriate-tooling-available)
  * [Running Integration Tests on Podman](#running-integration-tests-on-podman)
    * [DOCKER_HOST environment variable](#docker_host-environment-variable)
    * [Podman/Testcontainers incompatibility](#podmantestcontainers-incompatibility)
    * [macOS](#macos)
    * [Linux](#linux)
    * [Verify that the fix is effective](#verify-that-the-fix-is-effective)
  * [Running system tests locally](#running-system-tests-locally)
    * [Prerequisites](#prerequisites)
    * [Environment variables](#environment-variables)
    * [Launch system tests](#launch-system-tests)
    * [Jenkins pipeline for system tests](#jenkins-pipeline-for-system-tests)
  * [Rendering documentation](#rendering-documentation)
  * [Producing an Asciinema Cast](#producing-an-asciinema-cast)
  * [Continuous Integration](#continuous-integration)
    * [Using the GitHub CI workflows against a fork](#using-the-github-ci-workflows-against-a-fork)
  * [DCO Signoff](#dco-signoff)
* [Development Guide for Kroxylicious Operator](#development-guide-for-kroxylicious-operator)
  * [Hacking and Debugging](#hacking-and-debugging)
  * [Building the operator](#building-the-operator)
  * [Installing the operator](#installing-the-operator)
  * [Installing the operator](#installing-the-operator-1)
  * [Creating a `KafkaProxy`](#creating-a-kafkaproxy)
  * [Testing](#testing)
    * [System Tests](#system-tests)
* [Deprecation Policy](#deprecation-policy)
<!-- TOC -->

## Build status
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=kroxylicious_kroxylicious&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=kroxylicious_kroxylicious) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=kroxylicious_kroxylicious&metric=coverage)](https://sonarcloud.io/summary/new_code?id=kroxylicious_kroxylicious)

## Build Prerequisites

- [JDK](https://openjdk.org/projects/jdk/17/) (version 21 and above) - JDK
- [`mvn`](https://maven.apache.org/index.html) (version 3.6.3 and above) - [Apache Maven®](https://maven.apache.org)
- [`docker`](https://docs.docker.com/install/) or [`podman`](https://podman.io/docs/installation) - Docker or Podman

> :warning: **If you are using Podman please see [these notes](#running-integration-tests-on-podman) below**



## Build

JDK version 21 or newer, and [Apache Maven®](https://maven.apache.org) are required for building this project.

Kroxylicious targets language level 17, except for the `integrationtests` module
which targets 21 to access some new language features.

Build the project like this:

```shell
mvn clean verify
```

The running of the tests can be controlled with the following Maven properties:

| property           | description                                                                               |
|--------------------|-------------------------------------------------------------------------------------------|
| `-DskipUTs=true`   | skip unit tests                                                                           |
| `-DskipKTs=true`   | skip container image tests                                                                |
| `-DskipITs=true`   | skip integration tests                                                                    |
| `-DskipSTs=true`   | skip system tests                                                                         |
| `-DskipDTs=true`   | skip documentation tests                                                                  |
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
mvn clean package -Dquick
```

Run the following command to format the source code and organize the imports as per the project's conventions:

```shell
mvn process-sources
```

Build with the `dist` profile to create distribution artefacts (see [kroxylicious-app](kroxylicious-app)).
The distribution includes the Kroxylicious-maintained Filter implementations (located under [kroxylicious-additional-filters](./kroxylicious-additional-filters)).

```shell
mvn clean package -Pdist -Dquick
```

It is possible to omit the Kroxylicious-maintained Filter implementations by disabling the `withAdditionalFilters` profile.

```shell
mvn clean package -Pdist -Dquick -P-withAdditionalFilters
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

### Logging Conventions

We want to be careful with logging large amounts of data, for example stack traces, because we are potentially operating rapidly on many messages. Or Filters may be working at a 
per-record level, we could generate a huge amount of log data. But we often do not want to completely silence errors as it renders them invisible to the user.

Our convention is to use the slf4j fluent API to log once at a coarse level like WARN, but to only include cause Exceptions if the log level is set to a finer granularity:

```java
LOGGER.atWarn()
  .setCause(LOGGER.isDebugEnabled() ? failureCause : null) #only log the full stacktrace at debug
  .log("Something bad happened with failure message {}. Increase log level to DEBUG for stacktrace", failureCause.getMessage());
```

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
KROXYLICIOUS_CLASSPATH="/path/to/any.jar:/path/to/libs/dir/*" kroxylicious-app/target/kroxylicious-app-*-bin/kroxylicious-app-*/bin/kroxylicious-start.sh --config kroxylicious-app/example-proxy-config.yaml
```

### Debugging
Build with the `dist` profile as shown above.

To start in debug mode, listening on port `5005`:
```
JAVA_ENABLE_DEBUG=true kroxylicious-app/target/kroxylicious-app-*-bin/kroxylicious-app-*/bin/kroxylicious-start.sh -c kroxylicious-app/example-proxy-config.yaml
```
To suspend until debugger attaches:
```
JAVA_ENABLE_DEBUG=true JAVA_DEBUG_SUSPEND=true  kroxylicious-app/target/kroxylicious-app-*-bin/kroxylicious-app-*/bin/kroxylicious-start.sh -c kroxylicious-app/example-proxy-config.yaml
```
To change the debug port
```
JAVA_ENABLE_DEBUG=true JAVA_DEBUG_PORT=1234  kroxylicious-app/target/kroxylicious-app-*-bin/kroxylicious-app-*/bin/kroxylicious-start.sh -c kroxylicious-app/example-proxy-config.yaml
```
To change the root logger level
```
KROXYLICIOUS_ROOT_LOG_LEVEL=DEBUG kroxylicious-app/target/kroxylicious-app-*-bin/kroxylicious-app-*/bin/kroxylicious-start.sh -c kroxylicious-app/example-proxy-config.yaml
```
To customise the log4j2 config file edit:
```
vim kroxylicious-app/target/kroxylicious-app-*-bin/kroxylicious-app-*/config/log4j2.yaml
```
Low level network and frame logging is turned off by default for better performance. In case you want to debug, logging should be turned on in the `example-proxy-config.yaml` file:
```yaml
  logNetwork: true
  logFrames: true
```

## Building and pushing Kroxylicious Container Images

To build the proxy and operator image, first build the project using :

```shell
mvn -Pdist package
```

as Maven will be responsible to build the container images as tgz files.

Once the project is built you should be able to see `kroxylicious-operator.img.tar.gz` and `kroxylicious-proxy.img.tar.gz` in the `target` folder of `kroxylicious-operator` and `kroxylicious-app` directories.

Now if you want to push the Kroxylicious container and operator image to a specific registry like `quay.io` or `docker.io`, you can follow these steps:

**NOTE: Container runtime are illustrated using podman CLI. If you are using docker, replace podman with docker.**

First load the image from `tar.gz` file into podman daemon:

```shell
podman load <kroxylicious-operator.img.tar.gz-or-kroxylicious-proxy.img.tar.gz>
```

You can check the loaded image using:

```shell
podman images
```

Now you can tag the loaded image with the appropriate `quay.io` registry and username:

```shell
podman tag <loaded-image-name-or-id> quay.io/<your-username>/<repository-name>:<tag>
```

Once you have tagged the image, now you can push the image to `quay.io`:

```shell
podman push quay.io/<your-username>/<repository-name>:<tag>
```

Alternatively, to test locally made changes, push the built operator and proxy images into your Minikube.

```
minikube image load kroxylicious-operator/target/kroxylicious-operator.img.tar.gz --alsologtostderr=true 2>&1 | tail -n1
minikube image load kroxylicious-app/target/kroxylicious-proxy.img.tar.gz --alsologtostderr=true 2>&1 | tail -n1
```

> :warning: Some minikube container runtimes may not be able to load a gzipped tar (https://github.com/kubernetes/minikube/issues/21678), if the above commands report a failure 
> like `cache_images.go:265] failed pushing to: minikube`, then run:
> ```
> gunzip --to-stdout kroxylicious-operator/target/kroxylicious-operator.img.tar.gz | minikube image load - --alsologtostderr=true 2>&1 | tail -n1
> gunzip --to-stdout kroxylicious-app/target/kroxylicious-proxy.img.tar.gz | minikube image load - --alsologtostderr=true 2>&1 | tail -n1
> ```

## IDE setup

### Intellij

The project requires JDK-21 to build and run the `integrationtests` module and the IDEA project is configured to build against an SDK
named `temurin-21`. A suggested way to install this is with [sdkman](https://sdkman.io/) using `sdk install java 21-tem`.

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

It affects Linux and macOS.
On Linux it manifests as Http calls failing with a `Broken Pipe` exception. 
Similarly on macOS we see a `localhost:XXX failed to respond`.

To workaround around the issue, tune the `service_timeout` so that the timeout is in sympathy with the
expectations of the underlying HttpClient defaults.

Do so by following these instructions.

### macOS

Start the `podman` machine as normal, then:

```shell
echo 'mkdir -p /etc/containers/containers.conf.d && printf "[engine]\nservice_timeout=91\n" > /etc/containers/containers.conf.d/service-timeout.conf && systemctl restart podman.socket' |  podman machine ssh --username root --
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
On macOS, start this command:
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

## Running integration tests with IO_Uring

THe integration test suite enables IO_Uring un-conditionally which may trigger issues with memory limits. Certain platforms e.g. Fedora default to running with `RLIMIT_MEMLOCK` set.

If you see test failures such as 
```shell
[ERROR] Errors:
[ERROR]   MockServerTest.testClientCanSendAndReceiveRPCToMock:47 » IllegalState failed to create a child event loop
```
or 
```shell
java.lang.IllegalStateException: failed to create a child event loop
...
Caused by: java.lang.RuntimeException: failed to allocate memory for io_uring ring; try raising memlock limit (see getrlimit(RLIMIT_MEMLOCK, ...) or ulimit -l): Cannot allocate memory
```

Raise the `RLIMIT_MEMLOCK` (see https://lwn.net/Articles/876288/ for a discussion on the merits or otherwise of the default) by adding entries to `/etc/security/limits.conf` (see https://access.redhat.com/solutions/61334 for details on the file) the updates will take effect in the next login shell.
example config entry:
```text
* hard memlock unlimited
* soft memlock unlimited
```

## Running system tests locally

### Prerequisites
* minikube ([install guide](https://minikube.sigs.k8s.io/docs/start/))
* helm ([install guide](https://helm.sh/docs/helm/helm_install/))
* User must have access to a container registry such as [quay.io](https://quay.io) or [docker.io](https://docker.io).
     Create a public accessible repository within the registry named `kroxylicious`.
* [OPTIONAL] aws cli ([install guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)): in case an AWS Cloud account is used for KMS.

### Environment variables
* `KROXYLICIOUS_OPERATOR_REGISTRY`: url to the registry where the image of kroxylicious operator is located. Default value: `quay.io`
* `KROXYLICIOUS_OPERATOR_ORG`: name of the organisation in the registry where kroxylicious operator is located. Default value: `kroxylicious`
* `KROXYLICIOUS_OPERATOR_IMAGE_NAME`: name of the image of kroxylicious operator to be used. Default value: `operator`
* `ARCHITECTURE`: architecture of the cluster where the test clients are deployed. Default value: `System.getProperty("os.arch")`
* `KROXYLICIOUS_OPERATOR_VERSION`: version of kroxylicious operator to be used. Default value: `${project.version}` in pom file
* `KROXYLICIOUS_OPERATOR_INSTALL_DIR`: directory of the operator install files. Used for operator yaml installation. Default value: `System.getProperty("user.dir") + "/target/kroxylicious-operator-dist/install/"`
* `KROXYLICIOUS_IMAGE`: image location of the kroxylicious (proxy) image. Defaults to `quay.io/kroxylicious/kroxylicious:${project.version}`
* `KAFKA_VERSION`: kafka version to be used. Default value: `${kafka.version}` in pom file
* `STRIMZI_VERSION`: strimzi version to be used. Default value: `${strimzi.version}` in pom file
* `STRIMZI_NAMESPACE`: namespace used for strimzi cluster operator installation. Useful when strimzi is previously installed. Default value: `kafka`
* `SKIP_TEARDOWN`: variable for development purposes to avoid keep deploying and deleting deployments each run. Default value: `false`
* `SYNC_RESOURCES_DELETION`: variable for test diagnosis purposes to delete the resources synchronously. Default value: `false`
* `CONTAINER_CONFIG_PATH`: directory where `config.json` file is located. This file contains the pull secrets to be used by
the container engine. Default value: `$HOME/.docker/config.json`
* `SKIP_STRIMZI_INSTALL`: skip strimzi installation. Default value: `false`
* `KAFKA_CLIENT`: client used to produce/consume messages. Default value: `strimzi_test_client`. Currently supported values: `strimzi_test_client`, `kaf`, `kcat`, `python_test_client`
* `TEST_CLIENTS_IMAGE`: strimzi test client image to be used when running the tests. It is useful when running regression tests. Default value: `quay.io/strimzi-test-clients/test-clients:latest-kafka-${kafka.version}`
* `AWS_USE_CLOUD`: set to `true` in case AWS Cloud is used for Record Encryption System Tests. LocalStack will be used by default. Default value: `false`
* `AWS_REGION`: region of the AWS Cloud account to be used for KMS management. Default value: `us-east-2`
* `AWS_ACCESS_KEY_ID`: key id of the aws account with admin permissions to be used for KMS management. Mandatory when `AWS_USE_CLOUD` is `true`. Default value: `test`
* `AWS_SECRET_ACCESS_KEY`: secret access key of the aws account with admin permissions to be used for KMS management. Mandatory when `AWS_USE_CLOUD` is `true`. Default value: `test`
* `AWS_KROXYLICIOUS_ACCESS_KEY_ID`: key id of the aws account to be used for Kroxylicious config Map to encrypt/decrypt the messages. Mandatory when `AWS_USE_CLOUD` is `true`. Default value: `test`
* `AWS_KROXYLICIOUS_SECRET_ACCESS_KEY`: secret access key of the aws account to be used for Kroxylicious config Map to encrypt/decrypt the messages. Mandatory when `AWS_USE_CLOUD` is `true`. Default value: `test`
* `CURL_IMAGE`: curl image to be used in the corresponding arch for metrics tests. Default value: `mirror.gcr.io/curlimages/curl:8.16.0`

### Launch system tests
First of all, the code must be compiled and the distribution artifacts created:

```shell
mvn clean install -Dquick -Pdist
```

Start Minikube:
```shell
minikube start
```

By default, the system tests will pull the Operator from `quay.io/kroxylicious/operator:${project.version}` and the Proxy from `quay.io/kroxylicious/kroxylicious:${project.version}`.
These will be the latest images built by CI.  You can change this behaviour by setting the environment variables shown in the table above.

Alternatively, to run system tests against locally made changes, push the built operator and proxy images into your Minikube. Refer to section [Building and pushing Kroxylicious Container Images](#building-and-pushing-kroxylicious-container-images).

Run the system tests like this:

```
mvn clean verify -DskipITs=true -DskipUTs=true -DskipSTs=false -Pdist
```

### Jenkins pipeline for system tests

When a PR is created and the system tests are needed, if you are a member of
[Developers](https://github.com/orgs/kroxylicious/teams/developers), you may add the following comment into the PR to trigger the run.

```
@kroxylicious-robot run system tests
```

It will launch the `kroxylicious-system-tests-pr` build, that will insert a comment with a summary into the PR.

## Rendering documentation

For information on updating and rendering the documentation, see the `kroxylicious-docs` directory [README](kroxylicious-docs/README.md). 

## Producing an Asciinema Cast

There are some helper scripts that can reduce the manual work when producing an [asciinema](https://asciinema.org)
terminal cast.  There are a couple of scripts/programs that are used in consort.

* [extract-markdown-fencedcodeblocks.sh](./scripts/extract-markdown-fencedcodeblocks.sh) extracts fenced code blocks from a 
  Markdown document.  The list of commands is sent to stdout. The script also understands a non-standard extension to 
  the fenced code-block declaration: `prompt` assignments are treated as a comment that will proceed the command
  specified by fenced code-block. This can be used to provide narration.
  ````
     Lorem ipsum dolor sit amet
     ```shell { prompt="let's install the starnet client" }
        dnf install starnet-client
     ```
  ````
* [demoizer.sh](./scripts/demoizer.sh) takes a list of commands and executes each one.  It uses expect(1) to simulate
  a human typing the commands. It is designed to executed within the asciinema session.
* [asciinema-edit](https://github.com/cirocosta/asciinema-edit) used to quantise the periods of inactivity

The whole process looks like this:

```shell
# Extract the commands and narration
./scripts/extract-markdown-fencedcodeblocks.sh < path/to/some/markdown.md > /tmp/cmds
asciinema rec --overwrite --command './scripts/demoizer.sh /tmp/cmds .' demo.cast
# Uses quantize to reduce lengthy periods of inactivity resulting from awaits for resource to come ready etc.
asciinema-edit quantize --range 5 demo.cast > demo_processed.cast
asciinema upload demo_processed.cast
```

## Continuous Integration

We use Github actions for our build and release workflows. See [.github/AUTOMATION_README.md](.github/AUTOMATION_README.md) for information
about working with the actions.

### Using the GitHub CI workflows against a fork

All CI [workflows](.github/workflows) defined by the project are expected to execute within the context of a fork, apart from [docker workflow](.github/workflows/docker.yaml).
To enable the docker workflow, you need to configure three repository [variables](https://docs.github.com/en/actions/learn-github-actions/variables)
and one repository [secret](https://docs.github.com/en/actions/security-guides/using-secrets-in-github-actions).

* `REGISTRY_SERVER` variable - the server of the container registry service e.g. `quay.io` or `docker.io`
* `REGISTRY_ORGANISATION` variable - the organization of the container registry service e.g. `kroxylicious` or `yourusername`
* `PROXY_IMAGE_NAME` variable - the image name e.g. `kroxylicious`
* `OPERATOR_IMAGE_NAME` variable - the image name e.g. `operator`
* `REGISTRY_USERNAME` variable - your username on the service (or username of your robot account)
* `REGISTRY_TOKEN` secret - the access token that corresponds to `REGISTRY_USERNAME` 

The workflow will push the container image to `${REGISTRY_DESTINATION}` so ensure that the `${REGISTRY_USERNAME}` user has sufficient write privileges. 

## DCO Signoff

The project requires that all commits are signed-off, indicating that _you_ certify the changes with the [Developer
Certificate of Origin (DCO)](./DCO.txt).

This can be done using `git commit -s` for each commit
in your pull request. Alternatively, to signoff a bunch of commits you can use `git rebase --signoff _your-branch_`.

# Development Guide for Kroxylicious Operator

This is the development guide for Kroxylicious operator for Kubernetes.

## Hacking and Debugging

If you want to iterate quickly on the operator the simplest way is to run it as a process on your host (i.e. *not* running it within a Kubernetes cluster).

Note: The Integration Tests will only run if your kubectl context is pointing at a cluster. For development, we recommend using  `minikube`, for example:

```bash
minikube start --kubernetes-version=latest
````

You should now be able to run the tests using `mvn`.

If you want to run the `OperatorMain` (e.g. from your IDE, maybe for debugging) then you'll need to install the CRD:

```bash
kubectl apply -f kroxylicious-kubernetes-api/src/main/resources/META-INF/fabric8
```

You should now be able to play around with `KafkaProxy` CRs; read the "Creating a `KafkaProxy`" section.

Alternatively you can build the operator and run it within Kubernetes by following the instructions below.

## Building the operator

Refer to section [Building and pushing Kroxylicious Container Images](#building-and-pushing-kroxylicious-container-images).

## Installing the operator

Spin up a minikube custer:

```bash
minikube start --kubernetes-version=latest
````

## Installing the operator

```bash
kubectl apply -f kroxylicious-operator/target/packaged/install 
```

You can check that worked with something like

```bash
kubectl logs -n kroxylicious-operator deployment/kroxylicious-operator -c operator
```

## Creating a `KafkaProxy`

```bash
kubectl apply -f kroxylicious-operator/target/packaged/examples/simple/
```

You can check that worked with something like

```bash
kubectl get proxy simple -n my-proxy -o jsonpath='{.status}'
```

## Testing

To test things properly you'll need to point your virtual clusters at a running Kafka and also run a Kafka client so the proxy is handling some load.

### System Tests

The Kroxylicious system test suite uses the operator to deploy resources, so tests can be written in
java and executed locally in one's IDE. They do however require access to a Kubernetes clusters (usually minikube) and
helm, with the appropriate RBAC permissions to install operators and provision resources.

System tests are slow running things and are often difficult to diagnose issues just from external observation. To
support developers working with the operator while the tests execute the system test framework will enable remote debug
connections to the Kroxylicious Operator and create a LoadBalancer service (`debug-kroxylicious-operator`) to expose it.
Running `minikube tunnel` will make that available to the IDE, thus allowing developers to connect to the operator and
add breakpoints and step through execution. Note if we find ourselves doing this regularly we should look at improving
our unit test coverage and logging to make the diagnosis and avoidance of such issues much easier in less accessible
environments.

### Performance Tests

We have a set if low level micro benchmarks using JMH see their [readme](kroxylicious-microbenchmarks/README.md) for details on how to execute.

We also have a set of higher level benchmarks using a compose file to compare the performance of various Kroxylicious configurations and filters with a raw Kafka cluster.
To run the tests execute [perf-tests.sh](performance-tests/perf-tests.sh). The script uses variable to control behaviour, such as image versions. Of particular interest is the `TEST` variable to control which set of tests (by glob) are executed. To run `02-no-filters` test use something like `TEST=02.* ./performance-tests/perf-tests.sh`.

Note while we make every effort to support both `Podman` and `Docker` on an equal footing `perf-tests.sh` requires the use of **Docker compose** as we need support for `--wait` which is not currently available in podman. 

# Deprecation Policy

We want to let users know about upcoming changes to APIs and give them sufficient time to adapt. The following policy
describes how we'll do that.  It will apply until the project reaches its 1.0 release.

When there is an API deprecation, it must be announced in the [CHANGELOG](./CHANGELOG.md) of the coming release under
a section title "Changes, deprecations and removals".

Deprecated features become eligible for removal in the third minor release made following the release with the 
deprecation announcement.  There is an additional condition that at least three months must have elapsed too.  When
a deprecated feature is removed in a release, the removal should be documented under "Changes, deprecations and removals"
in the changelog.

Where technically possible, the production code should emit a warning if it detects the use of deprecated feature.  This
will serve to prompt the user to migrate to the new API.
