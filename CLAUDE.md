This is Kroxylicious, a Layer 7 proxy for the Kafka protocol.
It's written using Netty.
It uses the same `*RequestData` and `*ResponseData` classes which Apache Kafka does to represent protocol messages.

# Architecture

At its core the proxy uses the same setup as the proxy example code from the Netty project.

`KafkaProxyInitializer` sets up the pipeline when a Kafka client connects.
`KafkaProxyFrontendHandler` reads the requests from the client, and write requests to the client. 
`KafkaProxyBackendHandler` writes requests to the broker, and reads is responses. 
In the middle of the frontend pipeline is one or more `io.kroxylicious.proxy.internal.FilterHandler` instances.
A `FilterHandler` functions as an adapter for a "filter plugin".
Filter plugins are subcrete classes implementing one of the subinterfaces of `io.kroxylicious.proxy.filter.Filter`.
The direct sub-interfaces of `Filter` are:
* `io.kroxylicious.proxy.filter.RequestFilter` and `io.kroxylicious.proxy.filter.ResponseFilter`, which can receive all Kafka protocol messages.
* Kafka API-specific interfaces for each Kafka protocol API, like `MetadataRequestFilter`, or `FetchResponseFilter`.

Filters are instantiated using a `FilterFactory`.
The project includes a number of filter implementations. 
Many are used for testing purposes, but the ones in `kroxylicious-filters` are intended to be used by end users. 

The proxy also allows other plugins. See `TransportSubjectBuilderService`, `SaslSubjectBuilderService` as examples. 

Plugins can have plugins of their own.
For example, the `RecordEncryption` filter uses a `Kms` (key management system), which is a plugin with a number of implementations in the project.
Similarly the `Authorization` filter uses an `Authorizer` which is a plugin.

All plugins are loaded using the normal Java `java.util.ServiceLoader` mechanism.

In general, we expect and support end users writing their own plugin implementations. 
In other words, any plugin interface needs to be thoroughly documented and understandable by a reasonably competant Java developer.

## Configuration

A proxy instance is configured using a YAML configuration file. 
The plugins to be used will referred to in that YAML file, usually by fully-qualified or unqualifier class name.
Plugins usually accept YAML configuration of their own, which will be part of the main YAML configuration file.
Sometimes the YAML YAML configuration file will refer by name to other files which are also considered part of the proxy's configuration.
For example. this is used for security-sensitive configuration such as TLS keys and certificates, and also for the `AclAuthorizer`'s rules file.

## Deployment

The proxy is often deployed as a number of independent processes.
They don't know about each other. 
This has consequences in terms of some of the things a `Filter` can safely do.
In particular a Filter cannot assume other connections to the same cluster are happening through the same proxy process.
Thus a filter instance cannot assume it is intercepting all of the client-to-broker communication.

## Kubernetes

There is an Kubernetes operator for the proxy in `kroxylicious-operator`. 
The end user defines a number of custom resources, such a `KafkaProxy`, and `KafkaProxyIngress` and `KafkaProtocolFilter`.
the operator observes ("reconciles") these and creates/updates a Kubernetes `Deployment` to run a number of proxy instances as containers within `Pods`.

## API versus implementation

We make a clear distinction between "public API" and implementation.
"Public API" is anything an end user, or plugin developer is expected to touch.
This includes:

* All the plugin Java APIs (anything in `kroxylicious-api`, `kroxylicious-authorizer-api`, `kroxylicious-kms`
* The CLI interface of the proxy itself.
* The `CustomResourceDefinitions` for the Kubernetes operator in `kroxylicious-kubernetes-api`.

**Don't just make changes in these modules.** Any changes in any of these places is subject to a Proposal process, where the API specificed and agreed-to by the community. 
That said, genuinely trivial changes are OK, things like fixing typos, or making Javadoc clearer.

Other modules, such as `kroxylicous-runtime`, are implementation.
These can be changed as necessary, but even then the situation is not always clear.
Specifically, while the Java classes that are the Java representation of the parsed configuration file are not a public Java API,
the YAML configuration syntax itself _is_. 
So those classes can only be changed in a way that remains compatible with already existing configuration files which users might have written.

## End user docs

End user documentation lives in `kroxylicious-docs`. 
