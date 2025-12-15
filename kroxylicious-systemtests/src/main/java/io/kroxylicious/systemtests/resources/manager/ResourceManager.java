/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.manager;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.builder.Builder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.http.StandardHttpClient;
import io.skodjob.testframe.interfaces.ResourceType;
import io.skodjob.testframe.resources.ConfigMapType;
import io.skodjob.testframe.resources.CustomResourceDefinitionType;
import io.skodjob.testframe.resources.DeploymentType;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.resources.SecretType;
import io.skodjob.testframe.resources.ServiceType;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.systemtests.k8s.HelmClient;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;
import io.kroxylicious.systemtests.resources.kroxylicious.KroxyliciousResource;
import io.kroxylicious.systemtests.resources.strimzi.KafkaNodePoolType;
import io.kroxylicious.systemtests.resources.strimzi.KafkaType;

/**
 * The type Resource manager.
 */
public class ResourceManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceManager.class);

    private static ResourceManager instance;

    // Debug for https://github.com/kroxylicious/kroxylicious/issues/3065
    static {
        var shcLogger = LoggerFactory.getLogger(StandardHttpClient.class);
        LOGGER.warn("#3065: Debug enabled for {} : {}", shcLogger.getName(), shcLogger.isDebugEnabled());

        // Belt and brace - log the creates/deletes the resource manager thinks it is making.
        KubeResourceManager.get().addCreateCallback(hm -> {
            var metadata = hm.getMetadata();
            LOGGER.info("Created {}/{}/{}", hm.getKind(), metadata.getNamespace(), metadata.getName());
        });
        KubeResourceManager.get().addDeleteCallback(hm -> {
            var metadata = hm.getMetadata();
            LOGGER.info("Deleted {}/{}/{})", hm.getKind(), metadata.getNamespace(), metadata.getName());
        });
    }

    private ResourceManager() {
    }

    private static final ResourceType<?>[] resourceTypes = new ResourceType[]{
            new KafkaType(),
            new KafkaNodePoolType(),
            new ServiceType(),
            new ConfigMapType(),
            new DeploymentType(),
            new SecretType(),
            new CustomResourceDefinitionType(),
            new KroxyliciousResource<>(KafkaProxy.class),
            new KroxyliciousResource<>(KafkaService.class),
            new KroxyliciousResource<>(KafkaProxyIngress.class),
            new KroxyliciousResource<>(VirtualKafkaCluster.class),
            new KroxyliciousResource<>(KafkaProtocolFilter.class)
    };

    /**
     * Gets instance.
     *
     * @return the instance
     */
    public static synchronized ResourceManager getInstance() {
        if (instance == null) {
            instance = new ResourceManager();
            KubeResourceManager.get().setResourceTypes(resourceTypes);
        }
        return instance;
    }

    /**
     * Sets test context.
     *
     * @param context the context
     */
    public static void setTestContext(ExtensionContext context) {
        KubeResourceManager.get().setTestContext(context);
    }

    /**
     * Gets test context.
     *
     * @return the test context
     */
    public static ExtensionContext getTestContext() {
        return KubeResourceManager.get().getTestContext();
    }

    /**
     * Helm client.
     *
     * @return the helm client
     */
    public static HelmClient helmClient() {
        return KubeClusterResource.helmClusterClient();
    }

    /**
     * Create resource without wait.
     *
     * @param resources the resources
     */
    @SafeVarargs
    public final void createResourceWithoutWait(Builder<? extends HasMetadata>... resources) {
        KubeResourceManager.get().createResourceWithoutWait(Arrays.stream(resources).map(Builder::build).toList().toArray(new HasMetadata[0]));
    }

    /**
     * Create resource with wait.
     *
     * @param resources the resources
     */
    @SafeVarargs
    public final void createResourceFromBuilderWithWait(Builder<? extends HasMetadata>... resources) {
        KubeResourceManager.get().createOrUpdateResourceWithWait(Arrays.stream(resources).map(Builder::build).toList().toArray(new HasMetadata[0]));
    }

    /**
     * Create resource with wait.
     *
     * @param resources the resources
     */
    @SafeVarargs
    public final void createResourceFromBuilder(Builder<? extends HasMetadata>... resources) {
        KubeResourceManager.get()
                .createOrUpdateResourceWithWait(Arrays.stream(resources).filter(Objects::nonNull).map(Builder::build).toList().toArray(new HasMetadata[0]));
    }

    @SafeVarargs
    public final void createOrUpdateResourceWithWait(Builder<? extends HasMetadata>... resources) {
        KubeResourceManager.get()
                .createOrUpdateResourceWithWait(Arrays.stream(resources).filter(Objects::nonNull).map(Builder::build).toList().toArray(new HasMetadata[0]));
    }

    public <T extends HasMetadata> void replaceResourceWithRetries(T resource, Consumer<T> editor) {
        KubeResourceManager.get().replaceResourceWithRetries(resource, editor, 3);
    }
}
