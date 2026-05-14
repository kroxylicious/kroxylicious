/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;

import io.kroxylicious.proxy.tag.VisibleForTesting;
import io.kroxylicious.sidecar.v1alpha1.KroxyliciousSidecarConfig;
import io.kroxylicious.sidecar.v1alpha1.KroxyliciousSidecarConfigSpec;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.webhook.ProxyConfigGenerator.DEFAULT_NODE_ID_END;
import static io.kroxylicious.kubernetes.webhook.ProxyConfigGenerator.DEFAULT_NODE_ID_START;

/**
 * Watches {@link KroxyliciousSidecarConfig} resources and maintains an in-memory
 * cache for resolving the config applicable to a given namespace.
 */
class SidecarConfigResolver implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarConfigResolver.class);

    // namespace -> (name -> config)
    private final Map<String, Map<String, KroxyliciousSidecarConfig>> cache = new ConcurrentHashMap<>();
    @Nullable
    private final SharedIndexInformer<KroxyliciousSidecarConfig> informer;
    @Nullable
    private final SidecarConfigStatusUpdater statusUpdater;

    /**
     * Creates a resolver that watches all namespaces using the given client.
     * This blocks while the initial {@code KroxyliciousSidecarConfig} are loaded.
     */
    SidecarConfigResolver(
                          @NonNull KubernetesClient client,
                          @NonNull SidecarConfigStatusUpdater statusUpdater) {
        this.statusUpdater = statusUpdater;
        this.informer = client.resources(KroxyliciousSidecarConfig.class)
                .inAnyNamespace()
                .inform(new Handler()); // blocks for initial watch and list
    }

    /**
     * Creates a resolver with no informer and no status updater, for testing.
     */
    SidecarConfigResolver() {
        this(null);
    }

    /**
     * Creates a resolver with no informer, for testing.
     */
    @VisibleForTesting
    SidecarConfigResolver(@Nullable SidecarConfigStatusUpdater statusUpdater) {
        this.statusUpdater = statusUpdater;
        this.informer = null;
    }

    /**
     * The outcome of resolving a {@link KroxyliciousSidecarConfig} for a namespace.
     */
    record Resolution(
                      @NonNull Outcome outcome,
                      @NonNull Optional<KroxyliciousSidecarConfig> config) {

        enum Outcome {
            FOUND,
            NO_CONFIG,
            MULTIPLE_CONFIGS,
            INVALID_CONFIG
        }

        static Resolution found(@NonNull KroxyliciousSidecarConfig config) {
            return new Resolution(Outcome.FOUND, Optional.of(config));
        }

        static Resolution noConfig() {
            return new Resolution(Outcome.NO_CONFIG, Optional.empty());
        }

        static Resolution multipleConfigs() {
            return new Resolution(Outcome.MULTIPLE_CONFIGS, Optional.empty());
        }

        static Resolution invalidConfig() {
            return new Resolution(Outcome.INVALID_CONFIG, Optional.empty());
        }
    }

    /**
     * Resolves the sidecar config for the given namespace.
     *
     * <p>Resolution strategy:
     * <ol>
     *   <li>If {@code configName} is non-null, look up by exact name.</li>
     *   <li>If exactly one config exists in the namespace, use it.</li>
     *   <li>Otherwise, return empty.</li>
     * </ol>
     *
     * @param namespace the pod's namespace
     * @param configName optional explicit config name from pod annotation
     * @return the resolution outcome
     */
    @NonNull
    Resolution resolve(
                       @NonNull String namespace,
                       @Nullable String configName) {

        Map<String, KroxyliciousSidecarConfig> nsConfigs = cache.getOrDefault(namespace, Map.of());

        if (configName != null) {
            KroxyliciousSidecarConfig config = nsConfigs.get(configName);
            if (config == null) {
                LOGGER.atWarn()
                        .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                        .addKeyValue(WebhookLoggingKeys.NAME, configName)
                        .log("KroxyliciousSidecarConfig not found");
                return Resolution.noConfig();
            }
            return validatedResolution(config, namespace);
        }

        if (nsConfigs.size() == 1) {
            return validatedResolution(nsConfigs.values().iterator().next(), namespace);
        }

        if (nsConfigs.isEmpty()) {
            LOGGER.atDebug()
                    .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                    .log("No KroxyliciousSidecarConfig found in namespace");
            return Resolution.noConfig();
        }

        LOGGER.atWarn()
                .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                .addKeyValue("count", nsConfigs.size())
                .log("Multiple KroxyliciousSidecarConfig resources found, explicit annotation required");
        return Resolution.multipleConfigs();
    }

    @NonNull
    private Resolution validatedResolution(
                                           @NonNull KroxyliciousSidecarConfig config,
                                           @NonNull String namespace) {
        List<String> errors = validate(config);
        if (!errors.isEmpty()) {
            LOGGER.atWarn()
                    .addKeyValue(WebhookLoggingKeys.NAMESPACE, namespace)
                    .addKeyValue(WebhookLoggingKeys.NAME, config.getMetadata().getName())
                    .addKeyValue("errors", errors)
                    .log("KroxyliciousSidecarConfig failed validation");
            return Resolution.invalidConfig();
        }
        return Resolution.found(config);
    }

    /**
     * Adds a config to the cache directly, for testing.
     */
    @VisibleForTesting
    void put(@NonNull KroxyliciousSidecarConfig config) {
        String ns = config.getMetadata().getNamespace();
        String name = config.getMetadata().getName();
        cache.computeIfAbsent(ns, k -> new ConcurrentHashMap<>()).put(name, config);
    }

    /**
     * Simulates an informer add event, for testing.
     */
    @VisibleForTesting
    void simulateAdd(@NonNull KroxyliciousSidecarConfig config) {
        new Handler().onAdd(config);
    }

    /**
     * Simulates an informer update event, for testing.
     */
    @VisibleForTesting
    void simulateUpdate(
                        @NonNull KroxyliciousSidecarConfig oldConfig,
                        @NonNull KroxyliciousSidecarConfig newConfig) {
        new Handler().onUpdate(oldConfig, newConfig);
    }

    /**
     * Removes a config from the cache, for testing.
     */
    @VisibleForTesting
    void remove(@NonNull KroxyliciousSidecarConfig config) {
        String ns = config.getMetadata().getNamespace();
        String name = config.getMetadata().getName();
        Map<String, KroxyliciousSidecarConfig> nsConfigs = cache.get(ns);
        if (nsConfigs != null) {
            nsConfigs.remove(name);
            if (nsConfigs.isEmpty()) {
                cache.remove(ns);
            }
        }
    }

    @Override
    public void close() {
        if (informer != null) {
            informer.close();
        }
    }

    // Structural checks (spec null, virtualClusters empty, targetBootstrapServers blank) are expected
    // to be caught by CRD schema validation and should not be reachable in practice. They exist as a
    // defensive guard. The cross-field semantic checks (port collisions, range overflow) cannot be
    // expressed in the OpenAPI schema and are the primary reason this method exists.
    @VisibleForTesting
    static List<String> validate(@NonNull KroxyliciousSidecarConfig config) {
        List<String> errors = new ArrayList<>();
        KroxyliciousSidecarConfigSpec spec = config.getSpec();
        if (spec == null) {
            errors.add("spec is required");
            return errors;
        }
        if (spec.getVirtualClusters() == null || spec.getVirtualClusters().size() != 1) {
            errors.add("spec.virtualClusters must contain exactly one entry");
            return errors;
        }

        var vc = spec.getVirtualClusters().get(0);
        if (vc.getTargetBootstrapServers() == null || vc.getTargetBootstrapServers().isBlank()) {
            errors.add("spec.virtualClusters[0].targetBootstrapServers is required");
        }

        int bootstrapPort = ProxyConfigGenerator.resolveBootstrapPort(vc);
        int managementPort = ProxyConfigGenerator.resolveManagementPort(spec);

        var nodeIdRange = vc.getNodeIdRange();
        int nodeIdStart = nodeIdRange != null && nodeIdRange.getStartInclusive() != null
                ? nodeIdRange.getStartInclusive().intValue()
                : DEFAULT_NODE_ID_START;
        int nodeIdEnd = nodeIdRange != null && nodeIdRange.getEndInclusive() != null
                ? nodeIdRange.getEndInclusive().intValue()
                : DEFAULT_NODE_ID_END;

        if (nodeIdStart > nodeIdEnd) {
            errors.add("spec.virtualClusters[0].nodeIdRange.startInclusive (" + nodeIdStart
                    + ") must not exceed endInclusive (" + nodeIdEnd + ")");
        }

        int brokerPortCount = nodeIdEnd - nodeIdStart + 1;
        int firstBrokerPort = bootstrapPort + 1;
        int lastBrokerPort = firstBrokerPort + brokerPortCount - 1;

        if (lastBrokerPort > 65535) {
            errors.add("broker port range [" + firstBrokerPort + ", " + lastBrokerPort
                    + "] exceeds maximum port 65535");
        }

        if (bootstrapPort == managementPort) {
            errors.add("spec.virtualClusters[0].bootstrapPort (" + bootstrapPort
                    + ") must not equal spec.managementPort");
        }
        else if (managementPort >= firstBrokerPort && managementPort <= lastBrokerPort) {
            errors.add("spec.managementPort (" + managementPort
                    + ") conflicts with broker port range [" + firstBrokerPort + ", " + lastBrokerPort + "]");
        }

        return errors;
    }

    private void updateStatus(@NonNull KroxyliciousSidecarConfig config) {
        if (statusUpdater == null) {
            return;
        }
        List<String> errors = validate(config);
        if (errors.isEmpty()) {
            statusUpdater.setReady(config);
        }
        else {
            statusUpdater.setNotReady(config, String.join("; ", errors));
        }
    }

    private class Handler implements ResourceEventHandler<KroxyliciousSidecarConfig> {

        @Override
        public void onAdd(KroxyliciousSidecarConfig obj) {
            put(obj);
            updateStatus(obj);
            LOGGER.atInfo()
                    .addKeyValue(WebhookLoggingKeys.NAMESPACE, obj.getMetadata().getNamespace())
                    .addKeyValue(WebhookLoggingKeys.NAME, obj.getMetadata().getName())
                    .log("KroxyliciousSidecarConfig added");
        }

        @Override
        public void onUpdate(
                             KroxyliciousSidecarConfig oldObj,
                             KroxyliciousSidecarConfig newObj) {
            put(newObj);
            updateStatus(newObj);
            LOGGER.atInfo()
                    .addKeyValue(WebhookLoggingKeys.NAMESPACE, newObj.getMetadata().getNamespace())
                    .addKeyValue(WebhookLoggingKeys.NAME, newObj.getMetadata().getName())
                    .log("KroxyliciousSidecarConfig updated");
        }

        @Override
        public void onDelete(
                             KroxyliciousSidecarConfig obj,
                             boolean deletedFinalStateUnknown) {
            remove(obj);
            LOGGER.atInfo()
                    .addKeyValue(WebhookLoggingKeys.NAMESPACE, obj.getMetadata().getNamespace())
                    .addKeyValue(WebhookLoggingKeys.NAME, obj.getMetadata().getName())
                    .log("KroxyliciousSidecarConfig deleted");
        }
    }
}
