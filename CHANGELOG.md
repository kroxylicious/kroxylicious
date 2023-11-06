# CHANGELOG

Please enumerate **all user-facing** changes using format `<githib issue/pr number>: <short description>`, with changes ordered in reverse chronological order.

## 1.0.0

## 0.4.0

* [#701](https://github.com/kroxylicious/kroxylicious/pull/701): Bump org.apache.logging.log4j:log4j-bom from 2.21.0 to 2.21.1 #701

## 0.3.0

* [#686](https://github.com/kroxylicious/kroxylicious/pull/686): Bump org.apache.logging.log4j:log4j-bom from 2.20.0 to 2.21.0.
* [#634](https://github.com/kroxylicious/kroxylicious/issues/634): Update integration tests JDK dependency to 21.
* [#632](https://github.com/kroxylicious/kroxylicious/pull/632): Kroxylicious tester now supports creating & deleting topics on specific virtual clusters.
* [#675](https://github.com/kroxylicious/kroxylicious/pull/675): Bump to Netty 4.1.100.Final to mitigate the Rapid Reset Attack (CVE-2023-44487)
* [#665](https://github.com/kroxylicious/kroxylicious/pull/665): Bump org.apache.kafka:kafka-clients from 3.5.1 to 3.6.0
* [#660](https://github.com/kroxylicious/kroxylicious/pull/660): Use container registry neutral terminology in docs/scripts #660
* [#648](https://github.com/kroxylicious/kroxylicious/pull/648): Bump io.netty.incubator:netty-incubator-transport-native-io_uring from 0.0.22.Final to 0.0.23.Final
* [#649](https://github.com/kroxylicious/kroxylicious/pull/649): Bump io.netty:netty-bom from 4.1.97.Final to 4.1.99.Final
* [#650](https://github.com/kroxylicious/kroxylicious/pull/650): Bump io.sundr:builder-annotations from 0.100.3 to 0.101.0
* [#518](https://github.com/kroxylicious/kroxylicious/issues/518): [Breaking] #sendRequest ought to accept request header.
* [#623](https://github.com/kroxylicious/kroxylicious/pull/623): [Breaking] Refactor how Filters are Created
* [#633](https://github.com/kroxylicious/kroxylicious/pull/633): Address missing exception handling in FetchResponseTransformationFilter (and add unit tests)
* [#537](https://github.com/kroxylicious/kroxylicious/issues/537): Computation stages chained to the CompletionStage return by #sendRequest using the default executor async methods now run on the Netty Event Loop.
* [#612](https://github.com/kroxylicious/kroxylicious/pull/612): [Breaking] Allow filter authors to declare when their filter requires configuration. Note this includes a backwards incompatible change to the contract of the `Contributor`. `getInstance` will now throw exceptions rather than returning `null` to mean there was a problem or this contributor does not know about the requested type.
* [#608](https://github.com/kroxylicious/kroxylicious/pull/608): Improve the contributor API to allow it to express more properties about the configuration. This release deprecates `Contributor.getConfigType` in favour of `Contributor.getConfigDefinition`. It also removes the proliferation of ContributorManager classes by providing a single type which can handle all Contributors.
* [#538](https://github.com/kroxylicious/kroxylicious/pull/538): Refactor FilterHandler and fix several bugs that would leave messages unflushed to client/broker.
* [#531](https://github.com/kroxylicious/kroxylicious/pull/531): Simple Test Client now supports multi-RPC conversations with the server.
* [#510](https://github.com/kroxylicious/kroxylicious/pull/510): Add multi-tenant kubernetes example
* [#519](https://github.com/kroxylicious/kroxylicious/pull/519): Fix Kafka Client leaks in the SampleFilterIntegrationTest.
* [#494](https://github.com/kroxylicious/kroxylicious/issues/494): [Breaking] Make the Filter API fully asynchronous (filter methods must return a CompletionStage)
* [#498](https://github.com/kroxylicious/kroxylicious/issues/498): Include the cluster name from the configuration node in the config model.
* [#488](https://github.com/kroxylicious/kroxylicious/pull/488): Kroxylicious Bill Of Materials 
* [#480](https://github.com/kroxylicious/kroxylicious/issues/480): Multi-tenant - add suport for the versions of OffsetFetch, FindCoordinator, and DeleteTopics used by Sarama client v1.38.1
* [#472](https://github.com/kroxylicious/kroxylicious/issues/472): Respect logFrame/logNetwork options in virtualcluster config
* [#470](https://github.com/kroxylicious/kroxylicious/issues/470): Ensure that the EagerMetadataLearner passes on a client's metadata request with fidelity (fix for kcat -C -E)
* [#416](https://github.com/kroxylicious/kroxylicious/issues/416): Eagerly expose broker endpoints on startup to allow existing client to reconnect (without connecting to bootstrap).
* [#463](https://github.com/kroxylicious/kroxylicious/issues/463): deregister micrometer hooks, meters and the registry on shutdown
* [#443](https://github.com/kroxylicious/kroxylicious/pull/443): Obtain upstream ApiVersions when proxy is not SASL offloading
* [#412](https://github.com/kroxylicious/kroxylicious/issues/412): Remove $(portNumber) pattern from brokerAddressPattern for SniRouting and PortPerBroker schemes
* [#414](https://github.com/kroxylicious/kroxylicious/pull/414): Add kubernetes sample illustrating SNI based routing, downstream/upstream TLS and the use of certificates from cert-manager.
* [#392](https://github.com/kroxylicious/kroxylicious/pull/392): Introduce CompositeFilters
* [#401](https://github.com/kroxylicious/kroxylicious/pull/401): Fix netty buffer leak when doing a short-circuit response
* [#409](https://github.com/kroxylicious/kroxylicious/pull/409): Bump netty.version from 4.1.93.Final to 4.1.94.Final #409 
* [#374](https://github.com/kroxylicious/kroxylicious/issues/374) Upstream TLS support
* [#375](https://github.com/kroxylicious/kroxylicious/issues/375) Support key material in PEM format (X.509 certificates and PKCS-8 private keys) 
* [#398](https://github.com/kroxylicious/kroxylicious/pull/398): Validate admin port does not collide with cluster ports
* [#384](https://github.com/kroxylicious/kroxylicious/pull/384): Bump guava from 32.0.0-jre to 32.0.1-jre
* [#372](https://github.com/kroxylicious/kroxylicious/issues/372): Eliminate the test config model from the code-base
* [#364](https://github.com/kroxylicious/kroxylicious/pull/364): Add Dockerfile for kroxylicious

### Changes, deprecations and removals

The Filter API is refactored to be fully asynchronous.  Filter API methods such as `#onXxxxRequest` and `onXxxxResponse`
now are required to return a `CompletionStage<FilterResult>`. The `FilterResult` encapsulates the message to be
forwarded and carries orders (such as close the connection). The context provides factory methods for creating
`FilterResult` objects.

The default metrics port has changed from 9193 to 9190 to prevent port collisions

Filter Authors can now implement CompositeFilter if they want a single configuration block to contribute multiple Filters
to the Filter chain. This enables them to write smaller, more focused Filter implementations but deliver them as a whole
behaviour with a single block of configuration in the Kroxylicious configuration yaml. This interface is mutually exclusive
with RequestFilter, ResponseFilter or any specific message Filter interfaces.

In the kroxylicious config, the brokerAddressPattern parameter for the PortPerBroker scheme no longer accepts or requires
:$(portNumber) suffix.  In addition, for the SniRouting scheme the config now enforces that there is no port specifier
present on the brokerAddressPattern parameter. Previously, it was accepted but would lead to a failure later.

Kroxylicious configuration no longer requires a non empty `filters` list, users can leave it unset or configure in an empty
list of filters and Kroxylicious will proxy to the cluster successfully.

The Contributor API for creating filters has been significantly changed. 

- `FilterContributor` is renamed `FilterFactory`. 
- Filter Authors will now implement one FilterFactory implementation for each Filter implementation. So the cardinality is now one-to-one.
- We now identify which filter we want to load using it's class name or simple class name,
for example `io.kroxylicious.filter.SpecialFilter` or `SpecialFilter`.
- `FilterConstructContext` is renamed `FilterCreateContext`
- FilterExecutors is removed from FilterCreateContext and the `eventloop()` method is pulled up to FilterCreateContext.
- BaseConfig is removed and any Jackson deserializable type can be used as config.
- configuration is no longer part of the FilterCreateContext, it is supplied as a parameter to the `FilterFactory#createFilter(..)` method.

The names used to identify port-per-broker and sni-routing schemes in the Kroxylicious configuration have changed:
- `PortPerBroker` -> `PortPerBrokerClusterNetworkAddressConfigProvider`
- `SniRouting` -> `SniRoutingClusterNetworkAddressConfigProvider`

The names used to identify micrometer configuration hooks in configuration have changed:
- `CommonTagsContributor` -> `CommonTagsHook`
- `StandardBindersContributor` -> `StandardBindersHook`
- 
#### CVE Fixes
CVE-2023-44487 [#675](https://github.com/kroxylicious/kroxylicious/pull/675)
