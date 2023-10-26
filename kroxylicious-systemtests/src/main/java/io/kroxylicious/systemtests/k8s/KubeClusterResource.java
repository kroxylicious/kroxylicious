/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.k8s;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.k8s.cluster.KubeCluster;
import io.kroxylicious.systemtests.k8s.cmd.KubeCmdClient;

/**
 * A Junit resource which discovers the running cluster and provides an appropriate KubeClient for it,
 * for use with {@code @BeforeAll} (or {@code BeforeEach}.
 * For example:
 * <pre><code>
 *     public static KubeClusterResource testCluster = new KubeClusterResources();
 *
 *     &#64;BeforeEach
 *     void before() {
 *         testCluster.before();
 *     }
 * </code></pre>
 */
public class KubeClusterResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(KubeClusterResource.class);
    private KubeCluster kubeCluster;
    private KubeCmdClient cmdClient;
    private KubeClient client;
    private static KubeClusterResource kubeClusterResource;
    private String namespace;

    private KubeClusterResource() {
    }

    /**
     * Gets instance.
     *
     * @return the instance
     */
    public static synchronized KubeClusterResource getInstance() {
        if (kubeClusterResource == null) {
            kubeClusterResource = new KubeClusterResource();
            kubeClusterResource.setDefaultNamespace(cmdKubeClient().defaultNamespace());
            LOGGER.info("Cluster default namespace is {}", kubeClusterResource.getNamespace());
        }
        return kubeClusterResource;
    }

    /**
     * Sets default namespace.
     *
     * @param namespace the namespace
     */
    public void setDefaultNamespace(String namespace) {
        this.namespace = namespace;
    }

    /**
     * Gets namespace which is used in Kubernetes clients at the moment
     * @return Used namespace
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Provides appropriate CMD client for running cluster
     * @return CMD client
     */
    public static KubeCmdClient<?> cmdKubeClient() {
        return kubeClusterResource.cmdClient().namespace(kubeClusterResource.getNamespace());
    }

    /**
     * Provides appropriate CMD client with expected namespace for running cluster
     * @param inNamespace Namespace will be used as a current namespace for client
     * @return CMD client with expected namespace in configuration
     */
    public static KubeCmdClient<?> cmdKubeClient(String inNamespace) {
        return kubeClusterResource.cmdClient().namespace(inNamespace);
    }

    /**
     * Provides appropriate Kubernetes client for running cluster
     * @return Kubernetes client
     */
    public static KubeClient kubeClient() {
        return kubeClusterResource.client().namespace(kubeClusterResource.getNamespace());
    }

    /**
     * Provides appropriate Kubernetes client with expected namespace for running cluster
     * @param inNamespace Namespace will be used as a current namespace for client
     * @return Kubernetes client with expected namespace in configuration
     */
    public static KubeClient kubeClient(String inNamespace) {
        return kubeClusterResource.client().namespace(inNamespace);
    }

    /**
     * Cmd client kube cmd client.
     *
     * @return the kube cmd client
     */
    public synchronized KubeCmdClient cmdClient() {
        if (cmdClient == null) {
            cmdClient = cluster().defaultCmdClient();
        }
        return cmdClient;
    }

    /**
     * Client kube client.
     *
     * @return the kube client
     */
    public synchronized KubeClient client() {
        if (client == null) {
            this.client = cluster().defaultClient();
        }
        return client;
    }

    /**
     * Cluster kube cluster.
     *
     * @return the kube cluster
     */
    public synchronized KubeCluster cluster() {
        if (kubeCluster == null) {
            kubeCluster = KubeCluster.bootstrap();
        }
        return kubeCluster;
    }
}
