/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

/**
 * Common keys for structured logging in kroxylicious-runtime.
 */
public class RuntimeLoggingKeys {

    private RuntimeLoggingKeys() {
    }

    /**
     * Network address.
     */
    public static final String ADDRESS = "address";

    /**
     * Advertised address for cluster communication.
     */
    public static final String ADVERTISED_ADDRESS = "advertisedAddress";

    /**
     * Number of bytes already read from a buffer or stream.
     */
    public static final String ALREADY_READ = "alreadyRead";

    /**
     * Key that matches multiple candidates ambiguously.
     */
    public static final String AMBIGUOUS_KEY = "ambiguousKey";

    /**
     * Fully-qualified class name with annotations.
     */
    public static final String ANNOTATED_CLASS = "annotatedClass";

    /**
     * Annotation type or value.
     */
    public static final String ANNOTATION = "annotation";

    /**
     * Kafka API identifier.
     */
    public static final String API_ID = "apiId";

    /**
     * The Kafka API key identifying the request or response type.
     */
    public static final String API_KEY = "apiKey";

    /**
     * The Kafka protocol API version.
     */
    public static final String API_VERSION = "apiVersion";

    /**
     * The authorised identity after SASL authentication.
     */
    public static final String AUTHORIZED_ID = "authorizedId";

    /**
     * Backend handler component name.
     */
    public static final String BACKEND_HANDLER = "backendHandler";

    /**
     * Address to which a network service is bound.
     */
    public static final String BIND_ADDRESS = "bindAddress";

    /**
     * Network binder component.
     */
    public static final String BINDER = "binder";

    /**
     * HTTP or protocol message body.
     */
    public static final String BODY = "body";

    /**
     * Buffer or buffer pool identifier.
     */
    public static final String BUFFER = "buffer";

    /**
     * Number of readable bytes in a buffer.
     */
    public static final String BUFFER_READABLE_BYTES = "bufferReadableBytes";

    /**
     * Candidate values when multiple matches are found.
     */
    public static final String CANDIDATES = "candidates";

    /**
     * Unique Netty channel identifier.
     */
    public static final String CHANNEL_ID = "channelId";

    /**
     * Client hostname.
     */
    public static final String CLIENT_HOST = "clientHost";

    /**
     * Client port number.
     */
    public static final String CLIENT_PORT = "clientPort";

    /**
     * Closeable resource being managed.
     */
    public static final String CLOSEABLE = "closeable";

    /**
     * Class name that collides with another.
     */
    public static final String COLLIDING_CLASS = "collidingClass";

    /**
     * Execution context identifier.
     */
    public static final String CONTEXT = "context";

    /**
     * Correlation identifier.
     */
    public static final String CORRELATION = "correlation";

    /**
     * Netty channel handler context.
     */
    public static final String CTX = "ctx";

    /**
     * Current minimum API version.
     */
    public static final String CURRENT_MIN_VERSION = "currentMinVersion";

    /**
     * Whether request decoding is enabled.
     */
    public static final String DECODE_REQUEST = "decodeRequest";

    /**
     * Whether response decoding is enabled.
     */
    public static final String DECODE_RESPONSE = "decodeResponse";

    /**
     * Delegate component or handler.
     */
    public static final String DELEGATE = "delegate";

    /**
     * Destination network address.
     */
    public static final String DESTINATION_ADDRESS = "destinationAddress";

    /**
     * Destination port number.
     */
    public static final String DESTINATION_PORT = "destinationPort";

    /**
     * Message flow direction (request/response).
     */
    public static final String DIRECTION = "direction";

    /**
     * Downstream component or connection.
     */
    public static final String DOWNSTREAM = "downstream";

    /**
     * Correlation ID for downstream client requests.
     */
    public static final String DOWNSTREAM_CORRELATION_ID = "downstreamCorrelationId";

    /**
     * Size of encoded message in bytes.
     */
    public static final String ENCODED_SIZE = "encodedSize";

    /**
     * Network endpoint identifier.
     */
    public static final String ENDPOINT = "endpoint";

    /**
     * Error message or exception description.
     */
    public static final String ERROR = "error";

    /**
     * Filter component name or identifier.
     */
    public static final String FILTER = "filter";

    /**
     * Fully-qualified filter class name.
     */
    public static final String FILTER_CLASS = "filterClass";

    /**
     * Filter definition type.
     */
    public static final String FILTER_DEFINITION_TYPE = "filterDefinitionType";

    /**
     * Configured name of a filter instance.
     */
    public static final String FILTER_NAME = "filterName";

    /**
     * Kafka protocol frame.
     */
    public static final String FRAME = "frame";

    /**
     * Size of protocol frame in bytes.
     */
    public static final String FRAME_SIZE = "frameSize";

    /**
     * Type of protocol frame (request/response).
     */
    public static final String FRAME_TYPE = "frameType";

    /**
     * Frontend handler component name.
     */
    public static final String FRONTEND_HANDLER = "frontendHandler";

    /**
     * Gateway component or address.
     */
    public static final String GATEWAY = "gateway";

    /**
     * Protocol message header.
     */
    public static final String HEADER = "header";

    /**
     * Protocol header version number.
     */
    public static final String HEADER_VERSION = "headerVersion";

    /**
     * Hint or suggestion value.
     */
    public static final String HINT = "hint";

    /**
     * Incoming connection hostname.
     */
    public static final String INCOMING_HOST = "incomingHost";

    /**
     * Incoming connection port number.
     */
    public static final String INCOMING_PORT = "incomingPort";

    /**
     * JRE feature version number.
     */
    public static final String JRE_FEATURE_VERSION = "jreFeatureVersion";

    /**
     * Length of data or message.
     */
    public static final String LENGTH = "length";

    /**
     * Local network address.
     */
    public static final String LOCAL = "local";

    /**
     * Local network address.
     */
    public static final String LOCAL_ADDRESS = "localAddress";

    /**
     * Maximum frame size in bytes.
     */
    public static final String MAX_FRAME_SIZE_BYTES = "maxFrameSizeBytes";

    /**
     * Maximum supported API version.
     */
    public static final String MAX_VERSION = "maxVersion";

    /**
     * The SASL mechanism name (e.g., PLAIN, SCRAM-SHA-256).
     */
    public static final String MECHANISM = "mechanism";

    /**
     * Descriptive message text.
     */
    public static final String MESSAGE = "message";

    /**
     * Method name.
     */
    public static final String METHOD = "method";

    /**
     * Minimum supported API version.
     */
    public static final String MIN_VERSION = "minVersion";

    /**
     * Name of a component or resource.
     */
    public static final String NAME = "name";

    /**
     * New maximum API version after update.
     */
    public static final String NEW_MAX_VERSION = "newMaxVersion";

    /**
     * New minimum API version after update.
     */
    public static final String NEW_MIN_VERSION = "newMinVersion";

    /**
     * New name after renaming operation.
     */
    public static final String NEW_NAME = "newName";

    /**
     * Previous maximum API version before update.
     */
    public static final String OLD_MAX_VERSION = "oldMaxVersion";

    /**
     * Previous minimum API version before update.
     */
    public static final String OLD_MIN_VERSION = "oldMinVersion";

    /**
     * Previous name before renaming operation.
     */
    public static final String OLD_NAME = "oldName";

    /**
     * Original error message before wrapping or transformation.
     */
    public static final String ORIGINAL_ERROR = "originalError";

    /**
     * Output channel or destination.
     */
    public static final String OUT = "out";

    /**
     * Output stream or writer.
     */
    public static final String OUTPUT = "output";

    /**
     * Pause threshold in milliseconds.
     */
    public static final String PAUSE_THRESHOLD_MS = "pauseThresholdMs";

    /**
     * Pipeline component or identifier.
     */
    public static final String PIPELINE = "pipeline";

    /**
     * Fully-qualified plugin class name.
     */
    public static final String PLUGIN_CLASS = "pluginClass";

    /**
     * Plugin interface type.
     */
    public static final String PLUGIN_INTERFACE = "pluginInterface";

    /**
     * Network port number.
     */
    public static final String PORT = "port";

    /**
     * Predicate or filter condition.
     */
    public static final String PREDICATE = "predicate";

    /**
     * Fully-qualified principal class name.
     */
    public static final String PRINCIPAL_CLASS = "principalClass";

    /**
     * Authentication provider type.
     */
    public static final String PROVIDER_TYPE = "providerType";

    /**
     * Number of readable bytes.
     */
    public static final String READABLE = "readable";

    /**
     * Size of received frame in bytes.
     */
    public static final String RECEIVED_FRAME_SIZE_BYTES = "receivedFrameSizeBytes";

    /**
     * Remote network address.
     */
    public static final String REMOTE = "remote";

    /**
     * Remote network address.
     */
    public static final String REMOTE_ADDRESS = "remoteAddress";

    /**
     * Remote hostname.
     */
    public static final String REMOTE_HOST = "remoteHost";

    /**
     * Remote port number.
     */
    public static final String REMOTE_PORT = "remotePort";

    /**
     * Routing configuration or rule.
     */
    public static final String ROUTE = "route";

    /**
     * Service name or identifier.
     */
    public static final String SERVICE = "service";

    /**
     * The unique identifier for a client session with the proxy.
     */
    public static final String SESSION_ID = "sessionId";

    /**
     * Sleep interval in milliseconds.
     */
    public static final String SLEEP_INTERVAL_MS = "sleepIntervalMs";

    /**
     * SNI (Server Name Indication) hostname.
     */
    public static final String SNI_HOSTNAME = "sniHostname";

    /**
     * Source network address.
     */
    public static final String SOURCE_ADDRESS = "sourceAddress";

    /**
     * Source port number.
     */
    public static final String SOURCE_PORT = "sourcePort";

    /**
     * Current state of a component or connection.
     */
    public static final String STATE = "state";

    /**
     * State machine component.
     */
    public static final String STATE_MACHINE = "stateMachine";

    /**
     * Authenticated subject containing principals.
     */
    public static final String SUBJECT = "subject";

    /**
     * Supported cipher suites for TLS.
     */
    public static final String SUPPORTED_CIPHERS = "supportedCiphers";

    /**
     * Supported TLS protocol versions.
     */
    public static final String SUPPORTED_PROTOCOLS = "supportedProtocols";

    /**
     * Tags or labels associated with a resource.
     */
    public static final String TAGS = "tags";

    /**
     * Target state for a transition.
     */
    public static final String TARGET_STATE = "targetState";

    /**
     * JRE version being tested.
     */
    public static final String TESTED_JRE_VERSION = "testedJreVersion";

    /**
     * Cipher suite that is not supported.
     */
    public static final String UNSUPPORTED_CIPHER = "unsupportedCipher";

    /**
     * Protocol version that is not supported.
     */
    public static final String UNSUPPORTED_PROTOCOL = "unsupportedProtocol";

    /**
     * Upstream component or connection.
     */
    public static final String UPSTREAM = "upstream";

    /**
     * Correlation ID for upstream broker requests.
     */
    public static final String UPSTREAM_CORRELATION_ID = "upstreamCorrelationId";

    /**
     * Version compatibility status.
     */
    public static final String VERSION_STATUS = "versionStatus";

    /**
     * Virtual cluster name or identifier.
     */
    public static final String VIRTUAL_CLUSTER = "virtualCluster";
}
