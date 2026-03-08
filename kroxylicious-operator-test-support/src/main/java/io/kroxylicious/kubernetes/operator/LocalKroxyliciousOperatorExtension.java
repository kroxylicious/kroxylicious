/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.function.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.readiness.Readiness;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A JUnit 5 extension that runs a Kroxylicious operator locally for integration testing.
 * <p>
 * It manages the full lifecycle per test class: RBAC setup, namespace creation, CRD application,
 * operator startup, operator shutdown, and cleanup. It also exposes resource management methods
 * so that tests do not need to interact with Kubernetes directly.
 * <p>
 * All standard Kroxylicious CRDs ({@link KafkaProxy}, {@link VirtualKafkaCluster},
 * {@link KafkaService}, {@link KafkaProxyIngress}, {@link KafkaProtocolFilter}) are installed
 * automatically — callers do not need to list them.
 * <p>
 * Declare as a static field:
 * <pre>
 * {@code
 * @RegisterExtension
 * static LocalKroxyliciousOperatorExtension operator = LocalKroxyliciousOperatorExtension.builder()
 *         .withReconciler(new KafkaProxyReconciler(Clock.systemUTC(), SecureConfigInterpolator.DEFAULT_INTERPOLATOR))
 *         .withReconciler(new VirtualKafkaClusterReconciler(Clock.systemUTC(), DependencyResolver.create()))
 *         .build();
 * }
 * </pre>
 */
public class LocalKroxyliciousOperatorExtension implements BeforeAllCallback, AfterAllCallback, AfterEachCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalKroxyliciousOperatorExtension.class);
    private static final String DEFAULT_INSTALL_MANIFESTS_DIR = "packaging/install";
    static final String DEFAULT_CLUSTER_ROLE_GLOB = "*.ClusterRole.*.yaml";
    private static final String KROXYLICIOUS_IMAGE_ENV_VAR = "KROXYLICIOUS_IMAGE";
    private static final String KROXYLICIOUS_IMAGE_PROPERTIES = "/kroxylicious-image.properties";

    private final Supplier<LocallyRunningOperatorRbacHandler> rbacHandlerFactory;
    private final Function<LocallyRunningOperatorRbacHandler, LocallyRunOperatorExtension> extensionFactory;
    private final List<Executable> setupActions;
    private final List<Executable> teardownActions;
    private final List<Class<? extends HasMetadata>> additionalCleanupTypes;
    private final Runnable imagePreloader;

    // mutable per-class state
    private LocallyRunningOperatorRbacHandler rbacHandler;
    private LocallyRunOperatorExtension localOperatorExtension;
    private LocallyRunningOperatorRbacHandler.TestActor testActor;

    private LocalKroxyliciousOperatorExtension(Builder builder) {
        this(builder,
                () -> new LocallyRunningOperatorRbacHandler(builder.installManifestsDir, builder.clusterRoleGlobs.toArray(String[]::new)),
                handler -> buildExtension(List.copyOf(builder.reconcilers), List.copyOf(builder.additionalCRDs), handler),
                LocalKroxyliciousOperatorExtension::preloadOperandImage);
    }

    @VisibleForTesting
    LocalKroxyliciousOperatorExtension(Builder builder,
                                       Supplier<LocallyRunningOperatorRbacHandler> rbacHandlerFactory,
                                       Function<LocallyRunningOperatorRbacHandler, LocallyRunOperatorExtension> extensionFactory,
                                       Runnable imagePreloader) {
        if (builder.reconcilers.isEmpty()) {
            throw new IllegalStateException("at least one reconciler must be set");
        }
        this.rbacHandlerFactory = rbacHandlerFactory;
        this.extensionFactory = extensionFactory;
        this.setupActions = List.copyOf(builder.setupActions);
        this.teardownActions = List.copyOf(builder.teardownActions);
        this.additionalCleanupTypes = List.copyOf(builder.additionalCleanupTypes);
        this.imagePreloader = imagePreloader;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        rbacHandler = rbacHandlerFactory.get();
        rbacHandler.beforeEach(context);

        imagePreloader.run();

        for (var action : setupActions) {
            try {
                action.execute();
            }
            catch (Exception e) {
                throw e;
            }
            catch (Throwable t) {
                throw new AssertionError("Setup action failed", t);
            }
        }

        // LocallyRunOperatorExtension is built with oneNamespacePerClass=true, meaning
        // beforeAll/afterAll are the lifecycle methods that create and tear down the namespace
        // and start/stop the operator. This avoids per-test CRD deletion and re-application,
        // which is slow and can cause races when CRDs are still terminating.
        localOperatorExtension = extensionFactory.apply(rbacHandler);
        localOperatorExtension.beforeAll(context);

        testActor = rbacHandler.testActor(localOperatorExtension);
    }

    @Override
    public void afterAll(ExtensionContext context) {
        if (localOperatorExtension != null) {
            localOperatorExtension.afterAll(context);
        }
        for (var action : teardownActions) {
            try {
                action.execute();
            }
            catch (Throwable t) {
                throw new AssertionError("Teardown action failed", t);
            }
        }
        if (rbacHandler != null) {
            rbacHandler.afterEach(context);
            rbacHandler.afterAll(context);
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (testActor == null) {
            return;
        }
        // Clean up all Kroxylicious CRs and test-created resources between tests.
        // The namespace and CRDs are shared for the whole class and cleaned up in afterAll.
        deleteAll(VirtualKafkaCluster.class);
        deleteAll(KafkaProxy.class);
        deleteAll(KafkaProxyIngress.class);
        deleteAll(KafkaService.class);
        deleteAll(KafkaProtocolFilter.class);
        deleteAll(Secret.class);
        deleteAll(ConfigMap.class);
        additionalCleanupTypes.forEach(this::deleteAll);
    }

    private <T extends HasMetadata> void deleteAll(Class<T> type) {
        testActor.resources(type).list().getItems().forEach(testActor::delete);
    }

    private static void preloadOperandImage() {
        String image = resolveOperandImage();
        try (var client = OperatorTestUtils.kubeClient()) {
            var pod = client.run().withName("preload-operand-image")
                    .withNewRunConfig()
                    .withImage(image)
                    .withRestartPolicy("Never")
                    .withCommand("ls").done();
            try {
                client.resource(pod).waitUntilCondition(Readiness::isPodSucceeded, 2, TimeUnit.MINUTES);
            }
            finally {
                client.resource(pod).delete();
            }
        }
    }

    private static String sanitiseImageRef(String image) {
        return image.replaceAll("[\r\n]", "");
    }

    private static String resolveOperandImage() {
        var envImage = System.getenv(KROXYLICIOUS_IMAGE_ENV_VAR);
        if (envImage != null && !envImage.isBlank()) {
            envImage = sanitiseImageRef(envImage);
            LOGGER.info("Using Kroxylicious operand image ({}) from environment variable {}", envImage, KROXYLICIOUS_IMAGE_ENV_VAR);
            return envImage;
        }
        try (var is = LocalKroxyliciousOperatorExtension.class.getResourceAsStream(KROXYLICIOUS_IMAGE_PROPERTIES)) {
            if (is == null) {
                throw new IllegalStateException(
                        "No %s env var set and %s not found on classpath".formatted(KROXYLICIOUS_IMAGE_ENV_VAR, KROXYLICIOUS_IMAGE_PROPERTIES));
            }
            var props = new Properties();
            props.load(is);
            var image = props.getProperty("kroxylicious-image");
            if (image == null || image.isEmpty()) {
                throw new IllegalStateException("Property 'kroxylicious-image' not found in %s".formatted(KROXYLICIOUS_IMAGE_PROPERTIES));
            }
            image = sanitiseImageRef(image);
            LOGGER.info("Using Kroxylicious operand image ({}) from properties file {}", image, KROXYLICIOUS_IMAGE_PROPERTIES);
            return image;
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to read %s".formatted(KROXYLICIOUS_IMAGE_PROPERTIES), e);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static LocallyRunOperatorExtension buildExtension(List<Reconciler<?>> reconcilers,
                                                              List<Class<?>> additionalCRDs,
                                                              LocallyRunningOperatorRbacHandler rbacHandler) {
        var builder = LocallyRunOperatorExtension.builder()
                .withKubernetesClient(rbacHandler.operatorClient())
                .waitForNamespaceDeletion(false)
                .oneNamespacePerClass(true)
                .withConfigurationService(x -> x.withCloseClientOnStop(false));
        reconcilers.forEach(builder::withReconciler);
        // Always install all standard Kroxylicious CRDs — applyCrd is idempotent (createOrReplace)
        Stream.of(KafkaProxy.class, VirtualKafkaCluster.class, KafkaService.class,
                KafkaProxyIngress.class, KafkaProtocolFilter.class)
                .forEach(builder::withAdditionalCustomResourceDefinition);
        additionalCRDs.forEach(crd -> builder.withAdditionalCustomResourceDefinition((Class) crd));
        return builder.build();
    }

    /**
     * @return the Kubernetes namespace created for this test.
     */
    public String getNamespace() {
        return localOperatorExtension.getNamespace();
    }

    // ---- resource operations ----

    @NonNull
    public <T extends HasMetadata> T create(@NonNull T resource) {
        return testActor.create(resource);
    }

    @Nullable
    public <T extends HasMetadata> T get(@NonNull Class<T> type, @NonNull String name) {
        return testActor.get(type, name);
    }

    @NonNull
    public <T extends HasMetadata> T replace(@NonNull T resource) {
        return testActor.replace(resource);
    }

    @NonNull
    public <T extends HasMetadata> T patchStatus(@NonNull T resource) {
        return testActor.patchStatus(resource);
    }

    public <T extends HasMetadata> boolean delete(@NonNull T resource) {
        return testActor.delete(resource);
    }

    @NonNull
    public <T extends HasMetadata> NonNamespaceOperation<T, KubernetesResourceList<T>, Resource<T>> resources(@NonNull Class<T> type) {
        return testActor.resources(type);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final List<Reconciler<?>> reconcilers = new ArrayList<>();
        private final List<Class<?>> additionalCRDs = new ArrayList<>();
        private final List<Executable> setupActions = new ArrayList<>();
        private final List<Executable> teardownActions = new ArrayList<>();
        private final List<Class<? extends HasMetadata>> additionalCleanupTypes = new ArrayList<>();
        private String installManifestsDir = DEFAULT_INSTALL_MANIFESTS_DIR;
        private final List<String> clusterRoleGlobs = new ArrayList<>(List.of(DEFAULT_CLUSTER_ROLE_GLOB));

        private Builder() {
        }

        public Builder withReconciler(@NonNull Reconciler<?> reconciler) {
            this.reconcilers.add(reconciler);
            return this;
        }

        @SuppressWarnings("unused")
        public Builder withAdditionalCRD(@NonNull Class<?> crd) {
            this.additionalCRDs.add(crd);
            return this;
        }

        @SuppressWarnings("unused")
        public Builder withAdditionalCRDs(@NonNull Class<?>... crds) {
            this.additionalCRDs.addAll(Arrays.asList(crds));
            return this;
        }

        /**
         * Registers an action to run after RBAC setup but before the operator starts.
         * Useful for installing third-party CRDs that the operator watches.
         */
        @SuppressWarnings("unused")
        public Builder withSetupAction(@NonNull Executable action) {
            this.setupActions.add(action);
            return this;
        }

        /**
         * Registers an action to run after the operator stops.
         * Useful for cleaning up third-party CRDs installed via {@link #withSetupAction}.
         */
        @SuppressWarnings("unused")
        public Builder withTeardownAction(@NonNull Executable action) {
            this.teardownActions.add(action);
            return this;
        }

        /**
         * Registers additional resource types to delete between tests (in {@code afterEach}).
         * Use this for third-party CRD resources (e.g. Strimzi {@code Kafka}) that tests create
         * and that the extension does not clean up automatically.
         */
        @SuppressWarnings("unused")
        @SafeVarargs
        public final Builder withAdditionalCleanupTypes(@NonNull Class<? extends HasMetadata>... types) {
            this.additionalCleanupTypes.addAll(Arrays.asList(types));
            return this;
        }

        @SuppressWarnings("unused")
        public Builder withInstallManifestsDir(@NonNull String dir) {
            this.installManifestsDir = dir;
            return this;
        }

        /**
         * Replaces all cluster role glob(s) used to load RBAC rules with the given values.
         * If not called, the default is {@value DEFAULT_CLUSTER_ROLE_GLOB}.
         */
        public Builder replaceClusterRoleGlobs(@NonNull String... globs) {
            this.clusterRoleGlobs.clear();
            this.clusterRoleGlobs.addAll(Arrays.asList(globs));
            return this;
        }

        public LocalKroxyliciousOperatorExtension build() {
            return new LocalKroxyliciousOperatorExtension(this);
        }
    }
}
