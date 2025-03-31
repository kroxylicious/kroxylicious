/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.builder.Editable;
import io.fabric8.kubernetes.api.builder.VisitableBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.EditReplacePatchable;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.junit.AbstractOperatorExtension;

import io.kroxylicious.kubernetes.api.common.FilterRef;
import io.kroxylicious.kubernetes.api.common.FilterRefBuilder;
import io.kroxylicious.kubernetes.api.common.IngressRef;
import io.kroxylicious.kubernetes.api.common.IngressRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressSpecBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxySpec;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxySpecBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceSpecBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpecBuilder;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterSpec;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterSpecBuilder;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import info.schnatterer.mobynamesgenerator.MobyNamesGenerator;

import static io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP.Protocol.TCP;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static java.util.Objects.requireNonNull;

/**
 * Designed to cooperate with {@link io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension}
 * so that the operator runs locally with role based access restrictions as close to that it will
 * have when deployed to Kubernetes for real.
 * <br/>
 * Register this extension before LocallyRunOperatorExtension, passing LocallyRunOperatorExtension
 * the kubernetes client returned by {@link #operatorClient}.  This client will use a principal
 * which has been configured in Kubernetes to have the same RBAC rights as the operator will use
 * in production.
 * <br/>
 * For the test's interactions with Kubernetes, where more privileges are required, use the operations
 * exposed by {@link #testActor(AbstractOperatorExtension)}.
 */
public class LocallyRunningOperatorRbacHandler implements BeforeEachCallback, AfterEachCallback, AfterAllCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocallyRunningOperatorRbacHandler.class);

    // @formatter:off
    private static final ClusterRole FRAMEWORK_CLUSTER_ROLE = new ClusterRoleBuilder()
            .withNewMetadata()
            .withName("framework-cluster-role")
            .endMetadata()
            .addNewRule()
            .addToApiGroups("")
            .addToVerbs("get", "create", "delete", "patch")
            .addToResources("namespaces")
            .endRule()
            .addNewRule()
            .addToApiGroups("apiextensions.k8s.io")
            .addToVerbs("get", "list", "watch", "create", "delete", "patch", "delete")
            .addToResources("customresourcedefinitions")
            .endRule()
            .build();
    // @formatter:on

    private final String impersonatedUser = UUID.randomUUID().toString();

    private final KubernetesClient testActorClient = OperatorTestUtils.kubeClient();

    private final List<ClusterRole> clusterRoles;
    private final List<ClusterRoleBinding> roleBindings;

    public LocallyRunningOperatorRbacHandler(String resourceDirectory, String... clusterRoleFileGlobs) {
        this(Path.of(resourceDirectory), clusterRoleFileGlobs);
    }

    private LocallyRunningOperatorRbacHandler(Path resourceDirectory, String... clusterRoleFileGlobs) {
        requireNonNull(resourceDirectory);
        verifyClusterGlobs(clusterRoleFileGlobs);
        verifyDirectoryExists(resourceDirectory);
        clusterRoles = loadClusterRoles(resourceDirectory, clusterRoleFileGlobs);
        roleBindings = this.clusterRoles.stream().map(this::bindingForRole).toList();
    }

    private List<ClusterRole> loadClusterRoles(Path resourceDirectory, String[] clusterRoleFileGlobs) {
        var clusterRolePathMatchers = Arrays.stream(clusterRoleFileGlobs).map(g -> FileSystems.getDefault().getPathMatcher("glob:**/" + g)).toList();
        try (var adminClient = OperatorTestUtils.kubeClient();
                var files = Files.list(resourceDirectory)) {
            // The test framework itself needs these roles.
            Stream<ClusterRole> frameworkClusterRoles = Stream.of(FRAMEWORK_CLUSTER_ROLE);
            return Stream.concat(loadClusterRoles(files, adminClient, clusterRolePathMatchers), frameworkClusterRoles).toList();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void verifyClusterGlobs(String[] clusterRoleFileGlobs) {
        if (clusterRoleFileGlobs.length < 1) {
            throw new IllegalArgumentException("clusterRoleFileGlobs must not be empty");
        }
        Set<String> invalidGlobs = Arrays.stream(clusterRoleFileGlobs).filter(glob -> glob == null || glob.isEmpty()).collect(Collectors.toSet());
        if (!invalidGlobs.isEmpty()) {
            throw new IllegalArgumentException("clusterRoleFileGlobs contained invalid elements: " + invalidGlobs);
        }
    }

    private static void verifyDirectoryExists(Path resourceDirectory) {
        if (!Files.exists(resourceDirectory)) {
            throw new IllegalArgumentException("Resource directory does not exist: " + resourceDirectory);
        }
        if (!Files.isDirectory(resourceDirectory)) {
            throw new IllegalArgumentException("Resource directory is not a directory: " + resourceDirectory);
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        try (var adminClient = OperatorTestUtils.kubeClient()) {
            // The test framework itself needs these roles.
            clusterRoles.forEach(r -> {
                LOGGER.trace("Creating/patching: {}", name(r));
                adminClient.resource(r).createOr(EditReplacePatchable::patch);
            });

            roleBindings.forEach(roleBinding -> {
                LOGGER.trace("Creating role binding: {}", name(roleBinding));
                adminClient.resource(roleBinding).createOr(EditReplacePatchable::patch);
            });

            LOGGER.info("Applied Operator RBAC rules rewritten in terms of user {}.", impersonatedUser);
        }
    }

    private ClusterRoleBinding bindingForRole(ClusterRole clusterRole) {
        return new ClusterRoleBindingBuilder().withNewMetadata().withName(name(clusterRole) + "-" + impersonatedUser + "-binding").endMetadata()
                .addNewSubject().withKind("User").withName(impersonatedUser).withApiGroup("rbac.authorization.k8s.io").endSubject()
                .withNewRoleRef().withKind("ClusterRole").withName(name(clusterRole)).withApiGroup("rbac.authorization.k8s.io").endRoleRef().build();
    }

    private @NonNull Stream<ClusterRole> loadClusterRoles(Stream<Path> files, KubernetesClient adminClient, List<PathMatcher> clusterRolePathMatchers) {
        return files.filter(Files::isRegularFile).filter(path -> clusterRolePathMatchers.stream().anyMatch(matcher -> matcher.matches(path))).sorted()
                .flatMap(resourceFile -> {
                    try (var resourceInputStream = new FileInputStream(resourceFile.toString())) {
                        var resources = adminClient.load(resourceInputStream).items();
                        List<ClusterRole> roles = resources.stream().filter(ClusterRole.class::isInstance).map(ClusterRole.class::cast).toList();
                        return roles.stream();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException("failed to install cluster resources " + resourceFile, e);
                    }
                });
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        try (var adminClient = OperatorTestUtils.kubeClient()) {
            this.roleBindings.forEach(roleBinding -> {
                LOGGER.trace("Deleting ClusterRoleBinding: {}", name(roleBinding));
                adminClient.resource(roleBinding).delete();
            });

            this.clusterRoles.forEach(clusterRole -> {
                LOGGER.trace("Deleting ClusterRole: {}", name(clusterRole));
                adminClient.resource(clusterRole).delete();
            });
        }

    }

    @NonNull
    public KubernetesClient operatorClient() {
        return OperatorTestUtils.kubeClient(
                new KubernetesClientBuilder().editOrNewConfig().withImpersonateUsername(impersonatedUser).endConfig());
    }

    @NonNull
    public TestActor testActor(@NonNull AbstractOperatorExtension operatorExtension) {
        return new TestActor() {

            @NonNull
            @Override
            public <T extends HasMetadata> T create(@NonNull T resource) {
                return testActorClient.resource(resource).inNamespace(operatorExtension.getNamespace()).create();
            }

            @Nullable
            @Override
            public <T extends HasMetadata> T get(@NonNull Class<T> type, @NonNull String name) {
                return testActorClient.resources(type).inNamespace(operatorExtension.getNamespace()).withName(name).get();
            }

            @NonNull
            public <T extends HasMetadata> T replace(@NonNull T resource) {
                return testActorClient.resource(resource).inNamespace(operatorExtension.getNamespace()).update();
            }

            @Override
            public <T extends HasMetadata> boolean delete(@NonNull T resource) {
                var res = testActorClient.resource(resource).inNamespace(operatorExtension.getNamespace()).delete();
                return res.size() == 1 && res.get(0).getCauses().isEmpty();
            }

            @NonNull
            @Override
            public <T extends HasMetadata> NonNamespaceOperation<T, KubernetesResourceList<T>, Resource<T>> resources(
                                                                                                                      Class<T> type) {
                return testActorClient.resources(type).inNamespace(operatorExtension.getNamespace());
            }

        };
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        testActorClient.close();
    }

    public static class ResourceBuilder<SPEC_BUIlDER extends VisitableBuilder<SPEC, SPEC_BUIlDER>, SPEC extends Editable<SPEC_BUIlDER>, RESOURCE extends CustomResource<SPEC, ?>> {

        private final RESOURCE resource;
        private final TestActor testActor;

        public ResourceBuilder(RESOURCE emptyResource, ObjectMeta meta, TestActor testActor) {
            this.resource = emptyResource;
            this.testActor = testActor;
            this.resource.setMetadata(meta);
        }

        public ResourceBuilder<SPEC_BUIlDER, SPEC, RESOURCE> editSpec(Consumer<SPEC_BUIlDER> consumer) {
            SPEC_BUIlDER edit = resource.getSpec().edit();
            consumer.accept(edit);
            resource.setSpec(edit.build());
            return this;
        }

        public RESOURCE create() {
            return testActor.create(resource);
        }

        public RESOURCE replace() {
            return testActor.replace(resource);
        }

        public ResourceBuilder<SPEC_BUIlDER, SPEC, RESOURCE> withName(String name) {
            this.resource.setMetadata(this.resource.getMetadata().edit().withName(name).build());
            return this;
        }
    }

    public static class KafkaServiceResourceBuilder extends ResourceBuilder<KafkaServiceSpecBuilder, KafkaServiceSpec, KafkaService> {

        public KafkaServiceResourceBuilder(KafkaService emptyResource, ObjectMeta meta, TestActor testActor) {
            super(emptyResource, meta, testActor);
        }

        public KafkaServiceResourceBuilder withBootstrapServers(String bootstrapServers) {
            this.editSpec(b -> {
                b.withBootstrapServers(bootstrapServers);
            });
            return this;
        }
    }

    public static class KafkaProtocolFilterResourceBuilder extends ResourceBuilder<KafkaProtocolFilterSpecBuilder, KafkaProtocolFilterSpec, KafkaProtocolFilter> {

        public KafkaProtocolFilterResourceBuilder(KafkaProtocolFilter emptyResource, ObjectMeta meta, TestActor testActor) {
            super(emptyResource, meta, testActor);
        }

        public KafkaProtocolFilterResourceBuilder withArbitraryFilterConfig() {
            this.editSpec(specBuilder -> {
                specBuilder.withType("RecordValidation").withConfigTemplate(Map.of("rules", List.of(Map.of("allowNulls", false))));
            });
            return this;
        }
    }

    public static class KafkaIngressResourceBuilder extends ResourceBuilder<KafkaProxyIngressSpecBuilder, KafkaProxyIngressSpec, KafkaProxyIngress> {

        public KafkaIngressResourceBuilder(KafkaProxyIngress emptyResource, ObjectMeta meta, TestActor testActor) {
            super(emptyResource, meta, testActor);
        }

        public KafkaIngressResourceBuilder withClusterIpForProxy(KafkaProxy proxy) {
            this.editSpec(specBuilder -> {
                specBuilder.withNewClusterIP().withProtocol(TCP).endClusterIP().withNewProxyRef().withName(name(proxy)).endProxyRef();
            });
            return this;
        }

        public KafkaIngressResourceBuilder withClusterIpForProxyName(String proxyName) {
            this.editSpec(specBuilder -> {
                specBuilder.withNewClusterIP().withProtocol(TCP).endClusterIP().withNewProxyRef().withName(proxyName).endProxyRef();
            });
            return this;
        }
    }

    public static class VirtualKafkaClusterResourceBuilder extends ResourceBuilder<VirtualKafkaClusterSpecBuilder, VirtualKafkaClusterSpec, VirtualKafkaCluster> {

        public VirtualKafkaClusterResourceBuilder(VirtualKafkaCluster emptyResource, ObjectMeta meta, TestActor testActor) {
            super(emptyResource, meta, testActor);
        }

        public VirtualKafkaClusterResourceBuilder withProxyRef(KafkaProxy proxy) {
            this.editSpec(specBuilder -> specBuilder.withNewProxyRef().withName(name(proxy)).endProxyRef());
            return this;
        }

        public VirtualKafkaClusterResourceBuilder withProxyName(String proxyName) {
            this.editSpec(specBuilder -> specBuilder.withNewProxyRef().withName(proxyName).endProxyRef());
            return this;
        }

        public VirtualKafkaClusterResourceBuilder withTargetKafkaService(KafkaService service) {
            this.editSpec(specBuilder -> specBuilder.withNewTargetKafkaServiceRef().withName(name(service)).endTargetKafkaServiceRef());
            return this;
        }

        public VirtualKafkaClusterResourceBuilder withTargetKafkaServiceName(String serviceName) {
            this.editSpec(specBuilder -> specBuilder.withNewTargetKafkaServiceRef().withName(serviceName).endTargetKafkaServiceRef());
            return this;
        }

        public VirtualKafkaClusterResourceBuilder withFilters(KafkaProtocolFilter... filters) {
            List<FilterRef> refs = Arrays.stream(filters).map(f -> new FilterRefBuilder().withName(name(f)).build()).toList();
            this.editSpec(specBuilder -> specBuilder.withFilterRefs(refs));
            return this;
        }

        public VirtualKafkaClusterResourceBuilder withFiltersNamed(String... filterNames) {
            List<FilterRef> refs = Arrays.stream(filterNames).map(name -> new FilterRefBuilder().withName(name).build()).toList();
            this.editSpec(specBuilder -> specBuilder.withFilterRefs(refs));
            return this;
        }

        public VirtualKafkaClusterResourceBuilder withIngresses(KafkaProxyIngress... ingresses) {
            List<IngressRef> refs = Arrays.stream(ingresses).map(f -> new IngressRefBuilder().withName(name(f)).build()).toList();
            this.editSpec(specBuilder -> specBuilder.withIngressRefs(refs));
            return this;
        }

        public VirtualKafkaClusterResourceBuilder withIngressesNamed(String... ingressNames) {
            List<IngressRef> refs = Arrays.stream(ingressNames).map(name -> new IngressRefBuilder().withName(name).build()).toList();
            this.editSpec(specBuilder -> specBuilder.withIngressRefs(refs));
            return this;
        }
    }

    public interface TestActor {
        @NonNull
        <T extends HasMetadata> T create(@NonNull T resource);

        @Nullable
        <T extends HasMetadata> T get(@NonNull Class<T> type, @NonNull String name);

        @NonNull
        <T extends HasMetadata> T replace(@NonNull T resource);

        <T extends HasMetadata> boolean delete(@NonNull T resource);

        @NonNull
        <T extends HasMetadata> NonNamespaceOperation<T, KubernetesResourceList<T>, Resource<T>> resources(@NonNull Class<T> type);

        default ResourceBuilder<KafkaProxySpecBuilder, KafkaProxySpec, KafkaProxy> kafkaProxy() {
            return kafkaProxy(randomName());
        }

        default ResourceBuilder<KafkaProxySpecBuilder, KafkaProxySpec, KafkaProxy> kafkaProxy(String name) {
            KafkaProxy latest = get(KafkaProxy.class, name);
            if (latest == null) {
                return new ResourceBuilder<>(new KafkaProxyBuilder().withNewSpec().endSpec().build(),
                        new ObjectMetaBuilder().withName(name).build(),
                        this);
            }
            else {
                return new ResourceBuilder<>(latest, latest.getMetadata(), this);
            }
        }

        default KafkaProtocolFilterResourceBuilder protocolFilter() {
            return protocolFilter(randomName());
        }

        default KafkaProtocolFilterResourceBuilder protocolFilter(String name) {
            KafkaProtocolFilter latest = get(KafkaProtocolFilter.class, name);
            if (latest == null) {
                return new KafkaProtocolFilterResourceBuilder(new KafkaProtocolFilterBuilder().withNewSpec().endSpec().build(),
                        new ObjectMetaBuilder().withName(name).build(),
                        this);
            }
            else {
                return new KafkaProtocolFilterResourceBuilder(latest, latest.getMetadata(), this);
            }
        }

        default VirtualKafkaClusterResourceBuilder virtualKafkaCluster() {
            return virtualKafkaCluster(randomName());
        }

        default VirtualKafkaClusterResourceBuilder virtualKafkaCluster(String name) {
            VirtualKafkaCluster latest = get(VirtualKafkaCluster.class, name);
            if (latest == null) {
                return new VirtualKafkaClusterResourceBuilder(new VirtualKafkaClusterBuilder().withNewSpec().endSpec().build(),
                        new ObjectMetaBuilder().withName(name).build(),
                        this);
            }
            else {
                return new VirtualKafkaClusterResourceBuilder(latest, latest.getMetadata(), this);
            }
        }

        default KafkaIngressResourceBuilder ingress() {
            return ingress(randomName());
        }

        default KafkaIngressResourceBuilder ingress(String name) {
            KafkaProxyIngress latest = get(KafkaProxyIngress.class, name);
            if (latest == null) {
                return new KafkaIngressResourceBuilder(new KafkaProxyIngressBuilder().withNewSpec().endSpec().build(), new ObjectMetaBuilder().withName(name).build(),
                        this);
            }
            else {
                return new KafkaIngressResourceBuilder(latest, latest.getMetadata(), this);
            }
        }

        default KafkaServiceResourceBuilder kafkaService() {
            return kafkaService(randomName());
        }

        default KafkaServiceResourceBuilder kafkaService(String name) {
            KafkaService latest = get(KafkaService.class, name);
            if (latest == null) {
                return new KafkaServiceResourceBuilder(new KafkaServiceBuilder().withNewSpec().endSpec().build(), new ObjectMetaBuilder().withName(name).build(),
                        this);
            }
            else {
                return new KafkaServiceResourceBuilder(latest, latest.getMetadata(), this);
            }
        }

    }

    private static @NonNull String randomName() {
        return MobyNamesGenerator.getRandomName().replace("_", "-");
    }
}
