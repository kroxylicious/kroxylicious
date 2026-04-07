/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

/**
 * Common keys for use with structured logging.
 */
public class OperatorLoggingKeys {

    private OperatorLoggingKeys() {
    }

    public static final String NAMESPACE = "namespace";
    public static final String NAME = "name";
    public static final String KIND = "kind";
    public static final String ERROR = "error";
    /**
     * The checksum.
     */
    public static final String CHECKSUM = "checksum";

    /**
     * The cluster.
     */
    public static final String CLUSTER = "cluster";

    /**
     * The commit id.
     */
    public static final String COMMIT_ID = "commitId";

    /**
     * The config maps.
     */
    public static final String CONFIG_MAPS = "configMaps";

    /**
     * The default port.
     */
    public static final String DEFAULT_PORT = "defaultPort";

    /**
     * The env var.
     */
    public static final String ENV_VAR = "envVar";

    /**
     * The filter.
     */
    public static final String FILTER = "filter";

    /**
     * The filter name.
     */
    public static final String FILTER_NAME = "filterName";

    /**
     * The filter references.
     */
    public static final String FILTER_REFERENCES = "filterReferences";

    /**
     * The image.
     */
    public static final String IMAGE = "image";

    /**
     * The ingress.
     */
    public static final String INGRESS = "ingress";

    /**
     * The ingresses.
     */
    public static final String INGRESSES = "ingresses";

    /**
     * The interface.
     */
    public static final String INTERFACE = "interface";

    /**
     * The java vendor.
     */
    public static final String JAVA_VENDOR = "javaVendor";

    /**
     * The java version.
     */
    public static final String JAVA_VERSION = "javaVersion";

    /**
     * The kafka proxy.
     */
    public static final String KAFKA_PROXY = "kafkaProxy";

    /**
     * The kafka proxy name.
     */
    public static final String KAFKA_PROXY_NAME = "kafkaProxyName";

    /**
     * The kafka service.
     */
    public static final String KAFKA_SERVICE = "kafkaService";

    /**
     * The kafka service refs.
     */
    public static final String KAFKA_SERVICE_REFS = "kafkaServiceRefs";

    /**
     * The namespaces.
     */
    public static final String NAMESPACES = "namespaces";

    /**
     * The os arch.
     */
    public static final String OS_ARCH = "osArch";

    /**
     * The os name.
     */
    public static final String OS_NAME = "osName";

    /**
     * The os version.
     */
    public static final String OS_VERSION = "osVersion";

    /**
     * The port.
     */
    public static final String PORT = "port";

    /**
     * The properties file.
     */
    public static final String PROPERTIES_FILE = "propertiesFile";

    /**
     * The proxy ids.
     */
    public static final String PROXY_IDS = "proxyIds";

    /**
     * The resource.
     */
    public static final String RESOURCE = "resource";

    /**
     * The resource ids.
     */
    public static final String RESOURCE_IDS = "resourceIds";

    /**
     * The resource name.
     */
    public static final String RESOURCE_NAME = "resourceName";

    /**
     * The secondary class.
     */
    public static final String SECONDARY_CLASS = "secondaryClass";

    /**
     * The secret name.
     */
    public static final String SECRET_NAME = "secretName";

    /**
     * The secrets.
     */
    public static final String SECRETS = "secrets";

    /**
     * To.
     */
    public static final String TO = "to";

    /**
     * The user.
     */
    public static final String USER = "user";

    /**
     * The version.
     */
    public static final String VERSION = "version";

    /**
     * The virtual clusters.
     */
    public static final String VIRTUAL_CLUSTERS = "virtualClusters";

}
