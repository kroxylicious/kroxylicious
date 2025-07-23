# CHANGELOG

This changelog enumerates **all user-facing** changes made to Kroxylicious, in reverse chronological order.
For changes that effect a public API, the [deprecation policy](./DEV_GUIDE.md#deprecation-policy) is followed.

Format `<github issue/pr number>: <short description>`.

## SNAPSHOT

* [#2450](https://github.com/kroxylicious/kroxylicious/issues/2450): fix(proxy): Forward ApiVersions v0 response on UNSUPPORTED_VERSION v0 response from upstream
* [#2455](https://github.com/kroxylicious/kroxylicious/pull/2455): refactor: make oauth bearer validation filter content into a standalone guide.
* [#2378](https://github.com/kroxylicious/kroxylicious/issues/2378): refactor: Finish factoring out filter documentation into standalone guides.
* [#2412](https://github.com/kroxylicious/kroxylicious/pull/2412): [Operator] Add hostname/ip information to VKC loadbalancer status
* [#2314](https://github.com/kroxylicious/kroxylicious/pull/2314): Bump kubernetes-client.version from 7.2.0 to 7.3.1 (and Jackson from 2.18.3 to 2.19.1)
* [#2432](https://github.com/kroxylicious/kroxylicious/pull/2432): Bump io.micrometer:micrometer-bom from 1.15.0 to 1.15.2
* [#2431](https://github.com/kroxylicious/kroxylicious/pull/2431): Bump io.prometheus:prometheus-metrics-bom from 1.3.7 to 1.3.10
* [#2286](https://github.com/kroxylicious/kroxylicious/pull/2286): Bump apicurio-registry.version from 2.6.8.Final to 2.6.11.Final
* [#2414](https://github.com/kroxylicious/kroxylicious/pull/2414): Remove tcp and clusterNetworkAddressConfigProvider configuration options from virtual cluster
* [#2385](https://github.com/kroxylicious/kroxylicious/issues/2385) fix: Prevent existing proxy pod(s) rolling if number of replicas is changed.
* [#2464](https://github.com/kroxylicious/kroxylicious/pull/2464) Bump the log level for upstream fame and network loggers to match the downstream side. 

### Changes, deprecations and removals

* Remove deprecated `tls` and `clusterNetworkAddressConfigProvider` fields from virtual cluster. You must define
  at least one gateway in the `gateways` array of your virtual cluster instead.

## 0.13.0

* [#2346](https://github.com/kroxylicious/kroxylicious/issues/2346) fix: #2346: report failure to decrypt within the fetch response.
* [#1914](https://github.com/kroxylicious/kroxylicious/issues/1914) Remove deprecated AWS KMS service accessKey / secretKey config properties
* [#2240](https://github.com/kroxylicious/kroxylicious/issues/2240) Implement new message size distribution tracking metrics
* [#2241](https://github.com/kroxylicious/kroxylicious/issues/2241) Implement new connection counting metrics
* [#2239](https://github.com/kroxylicious/kroxylicious/issues/2239) Implement new message counting metrics
* [#2302](https://github.com/kroxylicious/kroxylicious/issues/2302) add build_info metric to Kroxylicious exposing version information
* [#2268](https://github.com/kroxylicious/kroxylicious/issues/2268) Ensure downstream connection is closed if proxy cannot match SNI hostname against a virtual cluster
* [#2185](https://github.com/kroxylicious/kroxylicious/pull/2185) Add $(virtualClusterName) placeholders to SNI bootstrap address and advertised broker address pattern
* [#2198](https://github.com/kroxylicious/kroxylicious/pull/2198) Require VirtualCluster name to be a valid DNS label
* [#2188](https://github.com/kroxylicious/kroxylicious/pull/2188) Delete deprecated bootstrapAddressPattern SNI gateway property
* [#2186](https://github.com/kroxylicious/kroxylicious/pull/2186) Remove deprecated FilterFactory implementations
* [#2164](https://github.com/kroxylicious/kroxylicious/issues/2164) Remove deprecated top-level configuration property filters
* [#1871](https://github.com/kroxylicious/kroxylicious/issues/1871) Remove deprecated configuration property bootstrap_servers

### Changes, deprecations and removals

* The deprecated top-level configuration property `filters` has been removed. Define filters using `filterDefinitions`
  Use `defaultFilters` (or the virtual cluster property `filters`) to assign filters to the virtual clusters.
* Removal the deprecated configuration property `bootstrap_servers` from the `targetCluster` object. Use `bootstrapServers`
  instead.
* Remove deprecated `MultiTenantTransformationFilterFactory`. Use `MultiTenant` instead.
* Remove deprecated `SampleProduceRequestFilterFactory`. Use `SampleProduceRequest` instead.
* Remove deprecated `SampleFetchResponseFilterFactory`. Use `SampleFetchResponse` instead.
* Remove deprecated `ProduceRequestTransformationFilterFactory`. Use `ProduceRequestTransformation` instead.
* Remove deprecated `FetchResponseTransformationFilterFactory`. Use `FetchResponseTransformation` instead.
* Remove deprecated `io.kroxylicious.proxy.config.tls.Tls(KeyProvider, TrustProvider)` constructor.
* Remove the deprecated configuration property `brokerAddressPattern` from `sniHostIdentifiesNode` gateway configuration. Use
  `advertisedBrokerAddressPattern` instead.
* VirtualCluster names are now restricted to a maximum length of 63, and must match pattern `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$` (case insensitive).
* `virtualClusters[].gateways[].sniHostIdentifiesNode.bootstrapAddress` can now contain an optional replacement token `$(virtualClusterName)`.
When this is present, it will be replaced with the name of that gateway's VirtualCluster.
* `virtualClusters[].gateways[].sniHostIdentifiesNode.advertisedBrokerAddressPattern` can now contain an optional replacement token `$(virtualClusterName)`.
When this is present, it will be replaced with the name of that gateway's VirtualCluster.
* All the existing metrics emitted by the proxy have been deprecated. They have been replaced with connection and message metrics.
  See the documentation for the details of the new metrics.
* Configuration the `AwsKms` directly with `accessKey` and `secretKey` config properties was deprecated at 0.9.0.  Support
  for this configuration is now removed.  Configure using a `longTermCredentials` object with `accessKeyId` and
  `secretAccessKey` properties instead.

## 0.12.0

* [#2135](https://github.com/kroxylicious/kroxylicious/pull/2135) Require client certificates by default if user supplies downstream trust
* [#2140](https://github.com/kroxylicious/kroxylicious/pull/2140) Bump Jackson from 2.18.1 to 2.18.3
* [#2115](https://github.com/kroxylicious/kroxylicious/pull/2115) Ensure request path chains deferred opaque requests correctly
* [#2113](https://github.com/kroxylicious/kroxylicious/pull/2113) Ensure that filter handler does not leak deferred opaque requests/responses if the upstream or downstream side closes unexpectedly 
* [#2098](https://github.com/kroxylicious/kroxylicious/pull/2098) Bump io.netty:netty-bom from 4.1.119.Final to 4.1.121.Final
* [#1928](https://github.com/kroxylicious/kroxylicious/pull/2085) Bump info.picocli:picocli from 4.7.6 to 4.7.7
* [#1437](https://github.com/kroxylicious/kroxylicious/issues/1437) Remove "zero-ack produce requests" warning
* [#1900](https://github.com/kroxylicious/kroxylicious/issues/1900) Enforce business rule that a proxy must have at least one virtual cluster
* [#1855](https://github.com/kroxylicious/kroxylicious/issues/1855) Upgrade to Apache Kafka 4.0
* [#1928](https://github.com/kroxylicious/kroxylicious/pull/1928) Make Kroxylicious Operator metrics available for collection

### Changes, deprecations and removals

* The default behaviour for client authentication has changed, if a Gateway is configured with client trust certificates, then
by default we will require the client to supply certificates. Previously the user had to also configure the clientAuth mode to
`REQUIRED` to enable this behaviour, the default was to not check the client certificates.

## 0.11.0

* [#1810](https://github.com/kroxylicious/kroxylicious/issues/1810) Run operator/operand image as non-root user
* [#1903](https://github.com/kroxylicious/kroxylicious/issues/1903) Rename `adminHttp` to `management` in the config model.
* [#1918](https://github.com/kroxylicious/kroxylicious/pull/1918)  Removes support for the deprecated config property `filePath`.
* [#1573](https://github.com/kroxylicious/kroxylicious/issues/1573) Minimal proxy health probe (livez)
* [#1847](https://github.com/kroxylicious/kroxylicious/pull/1847) Remodel virtual cluster map as a list (with explicit names).
* [#1840](https://github.com/kroxylicious/kroxylicious/pull/1840) Refactor virtual cluster configuration model
* [#1823](https://github.com/kroxylicious/kroxylicious/pull/1823) Allow VirtualClusters to express more than one listener
* [#1868](https://github.com/kroxylicious/kroxylicious/pull/1868) Support use of `$()` in KEK selector templates, deprecating `${}`
* [#1819](https://github.com/kroxylicious/kroxylicious/pull/1819) Bump io.netty:netty-bom from 4.1.117.Final to 4.1.118.Final
* [#1820](https://github.com/kroxylicious/kroxylicious/pull/1820) Bump io.micrometer:micrometer-bom from 1.14.3 to 1.14.4
* [#1768](https://github.com/kroxylicious/kroxylicious/pull/1768) Record Encryption: enable user to specify policy when we cannot resolve a Key for a topic
* [#1867](https://github.com/kroxylicious/kroxylicious/pull/1867) Capture metrics about the operation of the Kroxylicious Operator

### Changes, deprecations and removals

* In the `RecordEncryption` filter, `template` configuration property accepted by the `TemplateKekSelector` now supports the `$(topicName)` placeholder parameter. Use of `${topicName}` is deprecated and will be removed in a future release.
* The virtual cluster configuration properties `clusterNetworkAddressConfigProvider` and `tls` are deprecated.
  Define a named virtual cluster gateway within the `gateways` array.
* The networking schemes `PortPerNodeClusterNetworkAddressConfigProvider` and `RangeAwarePortPerNodeClusterNetworkAddressConfigProvider`
  are deprecated.  Define a virtual cluster gateway with `portIdentifiesNode` to express your networking requirements.
* The networking scheme `SniRoutingClusterNetworkAddressConfigProvider` is deprecated.  Define a virtual cluster gateway with
  `sniHostIdentifiesNode` to express your networking requirements.
* The `virtualClusters` configuration property now expects a list of `virtualCluster` objects (rather than a mapping
  of `name` to `virtualCluster`).  Furthermore, the `virtualCluster` object now requires a `name` configuration property.
  For backward compatibility, support for the map (and values without `name`) continues, but this will be removed in a future release.
* As announced at 0.5.0, when configuring TLS, the property `passwordFile` should be used for specifying location of a
  file providing the password. Support for the deprecated alias `filePath` is now removed.
* The `adminHttp` configuration property is renamed `management`.  The configuration property `host` within that object
  is renamed `bindAddress`.  Support for the old configuration property names is maintained, but their use is deprecated  
  and will be removed in a future release.

## 0.10.0

* [#1770](https://github.com/kroxylicious/kroxylicious/pull/1770) Name filter factories consistently
* [#1743](https://github.com/kroxylicious/kroxylicious/pull/1743) Apply TLS protocol and cipher suite restrictions to HTTP Clients used by KMS impls too
* [#1761](https://github.com/kroxylicious/kroxylicious/pull/1761) SNI exposition: user can control advertised broker port
* [#1766](https://github.com/kroxylicious/kroxylicious/issues/1766) Bump apicurio-registry.version from 2.6.6.Final to 2.6.7.Final
* [#1380](https://github.com/kroxylicious/kroxylicious/issues/1380) Deprecated FilterFactoryContext#eventLoop() is removed.
* [#1747](https://github.com/kroxylicious/kroxylicious/pull/1747) Bump io.micrometer:micrometer-bom from 1.14.2 to 1.14.3
* [#1745](https://github.com/kroxylicious/kroxylicious/pull/1745) Bump com.github.ben-manes.caffeine:caffeine from 3.1.8 to 3.2.0
* [#1006](https://github.com/kroxylicious/kroxylicious/pull/1660) Allow CipherSuites and TLS Protocols to be passed via Configuration
* [#1715](https://github.com/kroxylicious/kroxylicious/issues/1715) Deprecate `bootstrap_servers`, replacing it with `bootstrapServers` 
* [#1698](https://github.com/kroxylicious/kroxylicious/pull/1698) Bump netty.io_uring.version from 0.0.25.Final to 0.0.26.Final #1698
* [#1672](https://github.com/kroxylicious/kroxylicious/pull/1672) Limited Fortanix DSM backed KMS integration
* [#1709](https://github.com/kroxylicious/kroxylicious/pull/1709) Deprecate the existing top level `filters` configuration property; add support for named `filterDefinitions`, which can be scoped to a cluster.
* [#1643](https://github.com/kroxylicious/kroxylicious/pull/1643) Improve Encryption DEK co-ordination across threads
* [#1705](https://github.com/kroxylicious/kroxylicious/pull/1705) Replace usages of Contributor with new Plugin mechanism and delete Contributor

### Changes, deprecations and removals

* The factory for the Multitenancy filter is renamed from `MultiTenantTransformationFilterFactory` to `MultiTenant`. The
  old factory name is deprecated.
* The factories for the Kroxylicious Sample filters are renamed from `SampleProduceRequestFilterFactory` to 
  `SampleProduceRequest` and `SampleFetchResponseFilterFactory` to `SampleFetchResponse` respectively. The old factory
  names are now deprecated.
* The factories for the Kroxylicious Transform filters (used by the performance tests) are renamed from
  `ProduceRequestTransformationFilterFactory` to `ProduceRequestTransformation` and `FetchResponseTransformationFilterFactory`
* to `FetchResponseTransformation` respectively. The old factory names are now deprecated.
* The top level `filters` configuration property is deprecated. Configurations should use `filterDefinitions` and `defaultFilters` instead.
* The `bootstrap_servers` property of a virtual cluster's `targetCluster` is deprecated. It is replaced by a property called `bootstrapServers`.
* As per deprecation notice made at 0.7.0, `ProduceValidationFilterFactory` filter is removed.  Use `RecordValidation` instead.
* As per deprecation notice made at 0.7.0, `FilterFactoryContext#eventLoop()` is removed. Use `FilterFactoryContext#filterDispatchExecutor()` instead..
* SniRoutingClusterNetworkAddressConfigProvider configuration property `brokerAddressPattern` is deprecated. It is replaced by a property called
`advertisedBrokerAddressPattern`. These properties now also support the user optionally specifying a port, which will be the port advertised to
Kafka clients. This is to enable use-cases where Kroxylicious is behind some other proxy technology using a different port scheme.

## 0.9.0

* [#1668](https://github.com/kroxylicious/kroxylicious/pull/1668) Bump apicurio-registry.version from 2.6.5.Final to 2.6.6.Final
* [#1667](https://github.com/kroxylicious/kroxylicious/pull/1667) Bump io.micrometer:micrometer-bom from 1.14.1 to 1.14.2
* [#1666](https://github.com/kroxylicious/kroxylicious/pull/1666) Bump org.apache.logging.log4j:log4j-bom from 2.24.2 to 2.24.3 
* [#1294](https://github.com/kroxylicious/kroxylicious/issues/1294) AWS KMS - support authentication from instance metadata of EC2
* [#1657](https://github.com/kroxylicious/kroxylicious/pull/1657) Remove forwardPartialRequests feature of record validation filter
* [#1635](https://github.com/kroxylicious/kroxylicious/pull/1635) Handle ApiVersions unsupported version downgrade
* [#1648](https://github.com/kroxylicious/kroxylicious/pull/1648) Add test-only feature mechanism to Proxy configuration
* [#1379](https://github.com/kroxylicious/kroxylicious/issues/1379) Remove Deprecated EnvelopeEncryption
* [#1561](https://github.com/kroxylicious/kroxylicious/pull/1631) Allow Trust and ClientAuth to be set for Downstream TLS
* [#1550](https://github.com/kroxylicious/kroxylicious/pull/1550) Upgrade Apache Kafka from 3.8.0 to 3.9.0 #1550
* [#1557](https://github.com/kroxylicious/kroxylicious/pull/1557) Bump io.micrometer:micrometer-bom from 1.13.5 to 1.13.6
* [#1554](https://github.com/kroxylicious/kroxylicious/pull/1554) Bump apicurio-registry.version from 2.6.4.Final to 2.6.5.Final
* [#1522](https://github.com/kroxylicious/kroxylicious/pull/1522) Bump apicurio-registry.version from 2.6.3.Final to 2.6.4.Final
* [#1498](https://github.com/kroxylicious/kroxylicious/pull/1498) Give KmsService lifecycle methods
* [#1514](https://github.com/kroxylicious/kroxylicious/pull/1514) Bump io.netty:netty-bom from 4.1.112.Final to 4.1.113.Final
* [#1517](https://github.com/kroxylicious/kroxylicious/pull/1517) Bump apicurio-registry.version from 2.6.2.Final to 2.6.3.Final
* [#1515](https://github.com/kroxylicious/kroxylicious/pull/1515) Bump io.micrometer:micrometer-bom from 1.13.2 to 1.13.4

### Changes, deprecations and removals

* The deprecated EnvelopeEncryption filter is now removed.  Use RecordEncryption instead.
* The deprecated forwardPartialRequests option has been removed from the Record Validation Filter.
* This release upgrades Kroxylicious to Jackson 2.18 which "[improves](https://github.com/FasterXML/jackson-databind/issues/4785#issuecomment-2463105965)" how jackson handles constructor detection which may lead to issues with filter config.
If after the upgrade you observe issues similar to
`com.fasterxml.jackson.databind.exc.InvalidDefinitionException: Invalid type definition for type `com.fasterxml.jackson.databind.tofix.CreatorResolutionTest$HostPort`: Argument #0 of Creator [method com.fasterxml.jackson.databind.tofix.CreatorResolutionTest$HostPort#parse(java.lang.String)] has no property name (and is not Injectable): can not use as property-based Creator`
then you need to add `@JsonCreator(mode = JsonCreator.Mode.DELEGATING)` to the constructor one expects Jackson to use.


## 0.8.0

* [#1414](https://github.com/kroxylicious/kroxylicious/issues/1474) Enable hostname verification when connecting to upstream clusters using TLS 

## 0.7.0

* [#1414](https://github.com/kroxylicious/kroxylicious/issues/1414) Address record validation filter name inconsistency
* [#1348](https://github.com/kroxylicious/kroxylicious/issues/1348) Fix #1348: Rework the Record Encryption documentation describing the role of the administrator
* [#1415](https://github.com/kroxylicious/kroxylicious/pull/1415) Fix #1415: Improve record validation docs #1429
* [#1417](https://github.com/kroxylicious/kroxylicious/pull/1417): Extend JsonSchemaValidator to validate the incoming schema id matches the expected.
* [#1401](https://github.com/kroxylicious/kroxylicious/issues/1401): Support a FIPs-certified cipher from an alternative provider
* [#1416](https://github.com/kroxylicious/kroxylicious/pull/1416): Schema validation should not rely on the syntax validation
* [#1393](https://github.com/kroxylicious/kroxylicious/pull/1393): Remove api versions service
* [#1404](https://github.com/kroxylicious/kroxylicious/pull/1404): Move deprecated Context classes out of kroxylicious-api
* [#1402](https://github.com/kroxylicious/kroxylicious/pull/1402): Move FilterInvoker classes to kroxylicious-runtime
* [#1289](https://github.com/kroxylicious/kroxylicious/issues/1289): Record Encryption - expose maxEncryptionsPerDek for configuration
* [#1394](https://github.com/kroxylicious/kroxylicious/pull/1394): Make ClusterNetworkAddressConfigProvider and co internal
* [#1356](https://github.com/kroxylicious/kroxylicious/pull/1356): Changes for Kafka 3.8.0 #1356
* [#1354](https://github.com/kroxylicious/kroxylicious/pull/1354): Make EDEK cache refresh and expiry durations configurable
* [#1360](https://github.com/kroxylicious/kroxylicious/pull/1360): Bump kafka.version from 3.7.0 to 3.7.1
* [#1322](https://github.com/kroxylicious/kroxylicious/pull/1322): Introduce FilterDispatchExecutor
* [#1154](https://github.com/kroxylicious/kroxylicious/pull/1154): Apicurio based schema validation filter

### Changes, deprecations and removals

* The Record Encryption Filter now uses `AES/GCM/NoPadding` as the transformation String and checks the KMS 
returns a 256bit DEK. This enables users to configure an alternative JCE Provider in their JRE configuration
that offers this algorithm.
* FilterFactoryContext#eventLoop() is deprecated, replaced by FilterFactoryContext#filterDispatchExecutor().
This returns FilterDispatchExecutor, a new interface extending ScheduledExecutorService. FilterDispatchExecutor
has methods to enable Filters to check if the current thread is the Filter Dispatch Thread and it offers
specialized futures, where chained async methods will also run on the Filter Dispatch Thread when no executor
is supplied. This is intended to be a tool to make it convenient for Filters to hand off work to uncontrolled
threads, then switch back to an execution context where mutation of Filter members is safe.
* Record Encryption Filter: Data Encryption Keys will now be refreshed one hour after creation by default.
This is a bugfix for [#1139](https://github.com/kroxylicious/kroxylicious/issues/1339) to ensure we start
using new key material after key-encryption-keys are rotated in the KMS within some controlled duration.
* **Breaking changes to public kroxylicious-api module**, Filter Authors may be affected
  - Deprecated `io.kroxylicious.proxy.clusternetworkaddressconfigprovider.ClusterNetworkAddressConfigProviderContributor` moved to internal module
  - Deprecated `io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider` moved to internal module
  - Deprecated `io.kroxylicious.proxy.service.HostPort` moved to internal module
  - Deprecated `io.kroxylicious.proxy.filter.FilterInvoker` and implementations moved to internal module
  - Deprecated `io.kroxylicious.proxy.filter.FilterAndInvoker` moved to internal module
  - Deprecated `io.kroxylicious.proxy.filter.FilterInvokers` moved to internal module
  - Deprecated `io.kroxylicious.proxy.service.Context` moved to internal module
  - Deprecated `io.kroxylicious.proxy.service.Contributor` moved to internal module
  - Deprecated `io.kroxylicious.proxy.ApiVersionsService` removed without replacement

## 0.6.0

* [#1195](https://github.com/kroxylicious/kroxylicious/pull/1195): SASL OAUTHBEARER validation filter 
* [#1076](https://github.com/kroxylicious/kroxylicious/issues/1076): AWS KMS implementation for Record Encryption
* [#1201](https://github.com/kroxylicious/kroxylicious/pull/1201): Bump com.fasterxml.jackson:jackson-bom from 2.17.0 to 2.17.1
* [#1158](https://github.com/kroxylicious/kroxylicious/pull/1158): Bump io.netty:netty-bom from 4.1.108.Final to 4.1.109.Final
* [#1162](https://github.com/kroxylicious/kroxylicious/issues/1162): Fix #1162: allow tenant / resource name prefix separator to be controlled from configuration
* [#1120](https://github.com/kroxylicious/kroxylicious/pull/1120): Generate API compatability report as part of the release process.
* [#1220](https://github.com/kroxylicious/kroxylicious/pull/1220): Range-aware Port Per Node with integer configuration


### Changes, deprecations and removals

* RangeAwarePortPerNodeClusterNetworkAddressConfigProvider is a new ClusterNetworkAddressConfigProvider that is capable of modelling
more target topologies using a compact set of ports. Users can declare multiple ranges of node ids that exist in the target cluster
and the proxy will map those ranges on to a minimal set of proxy ports. See the [Virtual Cluster configuration docs](https://kroxylicious.io/kroxylicious/#_rangeawareportpernode_scheme)
for more information.
 
## 0.5.1

* [#1129](https://github.com/kroxylicious/kroxylicious/pull/1129): Ensure timeouts are cancelled when sendRequest completes normally. Thanks to @luozhenyu for spotting the issue.
* [#1115](https://github.com/kroxylicious/kroxylicious/pull/1115): Bump io.netty:netty-bom from 4.1.107.Final to 4.1.108.Final
* [#1110](https://github.com/kroxylicious/kroxylicious/pull/1110): Body decoder now supports older versions of ApiVersionsResponse
* [#1107](https://github.com/kroxylicious/kroxylicious/pull/1107): Replace deprecated FilePasswordFilePath class with @JsonAlias.
* [#1099](https://github.com/kroxylicious/kroxylicious/pull/1099): Bump io.micrometer:micrometer-bom from 1.12.3 to 1.12.4
* [#1103](https://github.com/kroxylicious/kroxylicious/pull/1103): Bump com.fasterxml.jackson:jackson-bom from 2.16.1 to 2.17.0
* [#1057](https://github.com/kroxylicious/kroxylicious/pull/1057): Check platform supports all record encryption ciphers at configuration time

## 0.5.0

* [#1074](https://github.com/kroxylicious/kroxylicious/pull/1074): Port-per-broker Exposition: make lowest broker id configurable
* [#1066](https://github.com/kroxylicious/kroxylicious/pull/1066): Log platform information on startup
* [#1050](https://github.com/kroxylicious/kroxylicious/pull/1050): Change AES GCM cipher to require a 256bit key
* [#1049](https://github.com/kroxylicious/kroxylicious/pull/1049): Add deprecated EnvelopeEncryption filter to ease migration to RecordEncryption filter
* [#1043](https://github.com/kroxylicious/kroxylicious/pull/1043): Rename EnvelopeEncryption filter to RecordEncryption
* [#1029](https://github.com/kroxylicious/kroxylicious/pull/1029): Upgrade to Kafka 3.7.0
* [#1011](https://github.com/kroxylicious/kroxylicious/pull/1011): Bump io.netty:netty-bom from 4.1.106.Final to 4.1.107.Final
* [#1010](https://github.com/kroxylicious/kroxylicious/pull/1010): Bump io.micrometer:micrometer-bom from 1.12.2 to 1.12.3
* [#1024](https://github.com/kroxylicious/kroxylicious/pull/1024): Log virtual cluster and metrics binding
* [#1032](https://github.com/kroxylicious/kroxylicious/pull/1032): Cache unknown alias resolutions temporarily
* [#1031](https://github.com/kroxylicious/kroxylicious/pull/1031): Fix inconsistently named configuration key in test filter class (FetchResponseTransformationFilter)
* [#1020](https://github.com/kroxylicious/kroxylicious/pull/1020): KMS retry logic failing with Null Pointers
* [#1019](https://github.com/kroxylicious/kroxylicious/pull/1019): Stop logging license header as part of the startup banner. 
* [#1004](https://github.com/kroxylicious/kroxylicious/pull/1004): Publish images to Quay kroxylicious/kroxylicious rather than kroxylicious-developer
* [#997](https://github.com/kroxylicious/kroxylicious/issues/997): Add hardcoded maximum frame size
* [#782](https://github.com/kroxylicious/kroxylicious/issues/782): Securely handle the HashiCorp Vault Token in Kroxylicious configuration
* [#973](https://github.com/kroxylicious/kroxylicious/pull/973): Remove deprecated CompositeFilter and its documentation
* [#935](https://github.com/kroxylicious/kroxylicious/pull/935): Enable user to configure alternative source of keys for vault KMS client
* [#787](https://github.com/kroxylicious/kroxylicious/issues/787): Initial documentation for the envelope-encryption feature.
* [#940](https://github.com/kroxylicious/kroxylicious/issues/940): Support vault namespaces and support secrets transit engine at locations other than /transit
* [#951](https://github.com/kroxylicious/kroxylicious/pull/951): Include the kroxylicious maintained filters in the dist by default
* [#910](https://github.com/kroxylicious/kroxylicious/pull/910): Envelope encryption preserve batches within MemoryRecords
* [#883](https://github.com/kroxylicious/kroxylicious/pull/883): Ensure we only initialise a filter factory once.
* [#912](https://github.com/kroxylicious/kroxylicious/pull/912): Bump io.netty:netty-bom from 4.1.104.Final to 4.1.106.Final
* [#909](https://github.com/kroxylicious/kroxylicious/pull/909): [build] use maven maven-dependency-plugin to detect missing/superfluous dependencies at build time
* [#895](https://github.com/kroxylicious/kroxylicious/pull/895): Ensure we execute deferred Filter methods on the eventloop
* [#896](https://github.com/kroxylicious/kroxylicious/issues/896): In TLS config, use passwordFile as property to accept password material from a file rather than filePath.
* [#844](https://github.com/kroxylicious/kroxylicious/pull/844): Fix connect to upstream using TLS client authentication
* [#885](https://github.com/kroxylicious/kroxylicious/pull/885): Bump kroxy.extension.version from 0.8.0 to 0.8.1

### Changes, deprecations and removals

* EncryptionVersion 1 is no longer supported, we found that it had diverged from our design document and have corrected it. From release 0.5.0 we guarantee backwards compatibility from EncryptionVersion 2 onwards.
* **We have renamed the EnvelopeEncryption filter** it is now the **RecordEncryption** filter. As this is a more accurate description of its role. We have not changed the way we deliver the encryption-at-rest as we are still using Envelope Encryption. Note we have preserved an `EnvelopeEncryption` factory, albeit deprecated, to avoid runtime failures for users upgrading from `0.4.x`. 
* When configuring TLS, the property `filePath` for specifying the location of a file providing the password is now
  deprecated.  Use `passwordFile` instead.
* When configuring TLS, it is no longer valid to pass a null inline password like `"storePassword": {"password": null}` instead use `"storePassword": null`
* As a result of the work of #909, some superfluous transitive dependencies have been removed from some kroxylicious.  If you were relying on those, you will need to
  adjust your dependencies as your adopt this release.
* `io.kroxylicious:kroxylicious-filter-test-support` now contains RecordTestUtils for creating example `Record`, `RecordBatch` and `MemoryRecords`. It also contains 
  assertj assertions for those same classes to enable us to write fluent assertions, accessible via `io.kroxylicious.test.assertj.KafkaAssertions`.
* The configuration for VaultKMS service has changed.
  * Instead of the `vaultUrl` config key, the provider now requires `vaultTransitEngineUrl`.  This must provide the
    complete path to the Transit Engine on the HashiCorp Vault instance (e.g. https://myvault:8200/v1/transit or
    https://myvault:8200/v1/mynamespace/transit).
  * The `vaultToken` field now requires a `PasswordProvider` object rather than inline text value.   You may pass the
    token from a file (filename specified by a `passwordFile` field) or inline (`password` field).  The latter is not
    recommended in production environments.
* The deprecated CompositeFilter interface has been removed.
* Container images for releases will be published to quay.io/kroxylicious/kroxylicious (rather than kroxylicious-developer)
* `FetchResponseTransformationFilter` now uses configuration key `transformationConfig` (rather than `config`). This matches
  the configuration expected by `ProduceRequestTransformationFilter`.

## 0.4.1

* [#836](https://github.com/kroxylicious/kroxylicious/pull/836): Cache decrypted EDEK and resolved aliases
* [#823](https://github.com/kroxylicious/kroxylicious/pull/823): Recover from EDEK decryption failures and improve KMS resilience measures
* [#841](https://github.com/kroxylicious/kroxylicious/issues/841): Ensure the envelope encryption filter transits record offsets unchanged.
* [#847](https://github.com/kroxylicious/kroxylicious/pull/847): Bump org.apache.maven.plugins:maven-compiler-plugin from 3.11.0 to 3.12.1
* [#838](https://github.com/kroxylicious/kroxylicious/issues/838): Ensure the decryption maintains record ordering, regardless of completion order of the decryptor.
* [#837](https://github.com/kroxylicious/kroxylicious/pull/837): refactor: take advantage of the topic injection in several integration tests including (the SampleFilterIT)
* [#827](https://github.com/kroxylicious/kroxylicious/issues/827): Release process should update version number references in container image versions too
* [#825](https://github.com/kroxylicious/kroxylicious/pull/825): Improve the topic encryption example
* [#832](https://github.com/kroxylicious/kroxylicious/pull/832): Bump io.netty:netty-bom from 4.1.101.Final to 4.1.104.Final
* [#828](https://github.com/kroxylicious/kroxylicious/pull/828): Bump io.micrometer:micrometer-bom from 1.12.0 to 1.12.1

## 0.4.0

* [#817](https://github.com/kroxylicious/kroxylicious/pull/817): Encryption Filter: Set hardcoded request timeout on Vault requests
* [#798](https://github.com/kroxylicious/kroxylicious/pull/798): Encryption Filter: Refactor Serialization to new Parcel Scheme
* [#809](https://github.com/kroxylicious/kroxylicious/pull/809): Bump Kroxylicious Junit Ext from 0.7.0 to 0.8.0
* [#803](https://github.com/kroxylicious/kroxylicious/pull/803): Bump kafka.version from 3.6.0 to 3.6.1 #803
* [#741](https://github.com/kroxylicious/kroxylicious/issues/741): Encryption Filter: Implement a HashiCorp Vault KMS 
* [#764](https://github.com/kroxylicious/kroxylicious/pull/764): Encryption Filter: Rotate to a new DEK when the old one is exhausted
* [#696](https://github.com/kroxylicious/kroxylicious/pull/696): Initial work on an Envelope Encryption Filter
* [#752](https://github.com/kroxylicious/kroxylicious/issues/752): Remove redundant re-installation of time-zone data in Dockerfile used for Kroxylicious container image
* [#727](https://github.com/kroxylicious/kroxylicious/pull/727): Tease out simple transform filters into their own module
* [#628](https://github.com/kroxylicious/kroxylicious/pull/628): Kroxylicious system tests
* [#738](https://github.com/kroxylicious/kroxylicious/pull/738): Update to Kroxylicious Junit Ext 0.7.0
* [#723](https://github.com/kroxylicious/kroxylicious/pull/723): Bump com.fasterxml.jackson:jackson-bom from 2.15.3 to 2.16.0 #723
* [#724](https://github.com/kroxylicious/kroxylicious/pull/724): Bump io.netty.incubator:netty-incubator-transport-native-io_uring from 0.0.23.Final to 0.0.24.Final
* [#725](https://github.com/kroxylicious/kroxylicious/pull/725): Bump io.netty:netty-bom from 4.1.100.Final to 4.1.101.Final #725
* [#710](https://github.com/kroxylicious/kroxylicious/pull/710): Rename modules
* [#709](https://github.com/kroxylicious/kroxylicious/pull/709): Add a KMS service API and an in-memory implementation
* [#667](https://github.com/kroxylicious/kroxylicious/pull/667): Nested factories
* [#701](https://github.com/kroxylicious/kroxylicious/pull/701): Bump org.apache.logging.log4j:log4j-bom from 2.21.0 to 2.21.1 #701

### Changes, deprecations and removals

* The `ProduceRequestTransformationFilter` and `FetchResponseTransformationFilter` have been moved to their own module kroxylicious-simple-transform.
  If you were depending on these filters, you must ensure that the kroxylicious-simple-transform JAR file is added to your classpath.  The
  Javadoc of these classes has been updated to convey the fact that these filters are *not* intended for production use.

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
