/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.mockito.stubbing.Answer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.DefaultManagedWorkflowAndDependentResourceContext;
import io.javaoperatorsdk.operator.processing.dependent.BulkDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResource;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.checksum.Crc32ChecksumGenerator;
import io.kroxylicious.kubernetes.operator.checksum.FixedChecksumGenerator;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.ProxyDeploymentDependentResource.KROXYLICIOUS_IMAGE_ENV_VAR;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class DerivedResourcesTest {

    static final ObjectMapper YAML_MAPPER = new YAMLMapper()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
            .enable(YAMLGenerator.Feature.LITERAL_BLOCK_STYLE)
            .registerModule(new JavaTimeModule());

    public static final Clock TEST_CLOCK = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));

    private static <T extends HasMetadata> T resourceFromFile(Path path, Class<T> valueType) {
        return resourcesFromFiles(Set.of(path), valueType).stream().findFirst().orElseThrow();
    }

    private static <T extends HasMetadata> List<T> resourcesFromFiles(Set<Path> paths, Class<T> valueType) {
        // TODO should validate against the CRD schema, because the DependentResource
        // should never see an invalid resource in production
        List<T> resources = paths.stream().map(Path::toFile).map(file -> {
            try {
                T resource = YAML_MAPPER.readValue(file, valueType);
                assertMinimalMetadata(resource.getMetadata(), file.toPath().toString());
                return resource;
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error reading " + file, e);
            }
        }).sorted(Comparator.comparing(ResourcesUtil::name)).toList();
        long uniqueResources = resources.stream().map(s -> namespace(s) + ":" + name(s)).distinct().count();
        // sanity check that the identifiers are unique
        assertThat(uniqueResources)
                .overridingErrorMessage("unexpected number of unique resources from files: %s", paths)
                .isEqualTo(paths.size());
        return resources;
    }

    @FunctionalInterface
    interface TriFunction<X, Y, Z, R> {
        R apply(X x, Y y, Z z);
    }

    /**
     * Abstraction for invoking the desired() method on a KubernetesDependentResource and BulkDependentResource.
     * This is needed because they have different return types ({@code R} vs {@code Map<R>}).
     * @param <P> The primary type (KafkaProxy)
     * @param <R> The resource type of the dependent resource (e.g. Service)
     */
    interface DesiredFn<P extends HasMetadata, R extends HasMetadata> {
        Class<R> resourceType();

        /**
         * @return A map of expected file name (i.e. a classpath resource which contains the YAML for the expected resource)
         * to actual resource
         */
        Map<String, R> invokeDesired(P primary, Context<P> context);

    }

    /**
     * Specialization of {@link DesiredFn} for KubernetesDependentResource
     * (i.e. where the desired() method returns an R).
     */
    record SingletonDependentResourceDesiredFn<D extends KubernetesDependentResource<R, P>, P extends HasMetadata, R extends HasMetadata>(
                                                                                                                                          D dependentResource,
                                                                                                                                          String dependentResourceKind,
                                                                                                                                          io.javaoperatorsdk.operator.processing.dependent.workflow.Condition<R, P> reconcilePrecondition,
                                                                                                                                          TriFunction<D, P, Context<P>, R> fn)
            implements DesiredFn<P, R> {
        @Override
        public Class<R> resourceType() {
            return dependentResource.resourceType();
        }

        @Override
        public Map<String, R> invokeDesired(P primary, Context<P> context) {
            if (reconcilePrecondition != null && !reconcilePrecondition.isMet(dependentResource, primary, context)) {
                return Map.of();
            }

            R apply = fn.apply(dependentResource, primary, context);
            return Map.of(name(apply), apply);
        }
    }

    /**
     * Specialization of {@link DesiredFn} for BulkDependentResource
     * (i.e. where the desired() method returns a Map<String, R>).
     */
    record BulkDependentResourceDesiredFn<D extends KubernetesDependentResource<R, P> & BulkDependentResource<R, P>, P extends HasMetadata, R extends HasMetadata>(
                                                                                                                                                                   D dependentResource,
                                                                                                                                                                   String dependentResourceKind,
                                                                                                                                                                   TriFunction<D, P, Context<P>, Map<String, R>> fn)
            implements DesiredFn<P, R> {

        @Override
        public Class<R> resourceType() {
            return dependentResource.resourceType();
        }

        @Override
        public Map<String, R> invokeDesired(P primary, Context<P> context) {
            return fn.apply(dependentResource, primary, context);
        }
    }

    @TestFactory
    @SetEnvironmentVariable(key = KROXYLICIOUS_IMAGE_ENV_VAR, value = "quay.io/kroxylicious/kroxylicious:test")
    Stream<DynamicContainer> dependentResourcesShouldEqual() {
        // Note that the order in this list should reflect the dependency order declared in the ProxyReconciler's
        // @ControllerConfiguration annotation, because the statefulness of Context<KafkaProxy> means that
        // later DependentResource can depend on Context state created by earlier DependentResources.

        var list = List.<DesiredFn<KafkaProxy, ?>> of(
                new SingletonDependentResourceDesiredFn<>(new ProxyConfigStateDependentResource(), "ConfigMap", null, ProxyConfigStateDependentResource::desired),
                new SingletonDependentResourceDesiredFn<>(new ProxyConfigDependentResource(), "ConfigMap", new ProxyConfigReconcilePrecondition(),
                        ProxyConfigDependentResource::desired),
                new SingletonDependentResourceDesiredFn<>(new ProxyDeploymentDependentResource(), "Deployment", null, ProxyDeploymentDependentResource::desired),
                new BulkDependentResourceDesiredFn<>(new ClusterServiceDependentResource(), "Service", ClusterServiceDependentResource::desiredResources));
        return dependentResourcesShouldEqual(list);
    }

    Stream<DynamicContainer> dependentResourcesShouldEqual(List<DesiredFn<KafkaProxy, ?>> list) {
        List<Path> paths = TestFiles.subDirectoriesForTest(DerivedResourcesTest.class);
        return paths.stream()
                .map(testDir -> {
                    String testCase = fileName(testDir);
                    try {
                        return DynamicContainer.dynamicContainer(testCase, testsForDir(list, testDir));
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException("For test directory " + testDir, e);
                    }

                });

    }

    @NonNull
    private static List<DynamicTest> testsForDir(List<DesiredFn<KafkaProxy, ?>> dependentResources,
                                                 Path testDir)
            throws IOException {
        try {
            var unusedFiles = TestFiles.childFilesMatching(testDir, "*");
            Path input = testDir.resolve("in-KafkaProxy.yaml");
            KafkaProxy kafkaProxy = resourceFromFile(input, KafkaProxy.class);
            List<VirtualKafkaCluster> virtualKafkaClusters = resourcesFromFiles(TestFiles.childFilesMatching(testDir, "in-VirtualKafkaCluster-*"),
                    VirtualKafkaCluster.class);
            List<KafkaService> kafkaServiceRefs = resourcesFromFiles(TestFiles.childFilesMatching(testDir, "in-KafkaService-*"), KafkaService.class);
            List<KafkaProxyIngress> ingresses = resourcesFromFiles(TestFiles.childFilesMatching(testDir, "in-KafkaProxyIngress-*"), KafkaProxyIngress.class);

            unusedFiles.remove(input);
            unusedFiles.removeAll(TestFiles.childFilesMatching(testDir, "in-*"));

            Context<KafkaProxy> context;
            try {
                context = buildContext(testDir, kafkaProxy, virtualKafkaClusters, kafkaServiceRefs, ingresses);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            List<DynamicTest> tests = new ArrayList<>();

            var dr = dependentResources.stream()
                    .flatMap(r -> r.invokeDesired(kafkaProxy, context).values()
                            .stream()
                            .map(x -> Map.entry(r.resourceType(), x)))
                    .collect(Collectors.groupingBy(Map.Entry::getKey))
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                            e -> e.getValue().stream().map(Map.Entry::getValue).collect(Collectors.toCollection(() -> new TreeSet<>(
                                    Comparator.comparing(ResourcesUtil::name))))));
            for (var entry : dr.entrySet().stream().sorted(Comparator.comparing(entry -> entry.getKey().getSimpleName())).toList()) {
                var resourceType = entry.getKey();
                var actualResources = entry.getValue();
                for (var actualResource : actualResources) {
                    String kind = resourceType.getSimpleName();
                    String name = name(actualResource);
                    var expectedFile = testDir.resolve("out-" + kind + "-" + name + ".yaml");
                    tests.add(DynamicTest.dynamicTest(kind + " '" + name + "' should have the same content as " + testDir.relativize(expectedFile),
                            () -> {
                                assertThat(Files.exists(expectedFile)).describedAs(expectedFile + " does not exist").isTrue();
                                var expected = loadExpected(expectedFile, resourceType);
                                assertSameYaml(actualResource, expected);
                                if (actualResource instanceof ConfigMap cm && cm.getData().containsKey("proxy-config.yaml")) {
                                    assertThatCode(() -> {
                                        parseConfig(cm.getData().get("proxy-config.yaml"));
                                    }).describedAs("proxy-config in configmap should be parseable").doesNotThrowAnyException();
                                }
                                unusedFiles.remove(expectedFile);

                            }));
                }
            }

            tests.add(DynamicTest.dynamicTest("There should be no unused files in " + testDir,
                    () -> assertThat(unusedFiles).isEmpty()));
            return tests;
        }
        catch (AssertionError e) {
            return List.of(DynamicTest.dynamicTest("failed to initialize test", () -> {
                throw e;
            }));
        }
    }

    private static void parseConfig(String content) {
        try {
            ConfigParser.createBaseObjectMapper().readValue(content, Configuration.class);
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Couldn't parse configuration", e);
        }
    }

    private static void assertMinimalMetadata(ObjectMeta metadata, String inFileName) {
        // sanity check since we can omit fields that the k8s API will ensure are present in reality
        assertThat(metadata.getName()).describedAs("metadata.name in " + inFileName).isNotNull().isNotEmpty();
        assertThat(metadata.getNamespace()).describedAs("metadata.namespace in " + inFileName).isNotNull().isNotEmpty();
    }

    private static <T> void assertSameYaml(T actualResource, T expected) throws JsonProcessingException {
        if (!expected.equals(actualResource)) {
            // Failing with a String-based assert makes it **much** easier to understand what the diffs are
            // because we're comparing YAML strings, rather than the resources.toString()
            // which is not normally YAML. It also means you can use copy&paste to update expected YAML.
            assertThat(YAML_MAPPER.writeValueAsString(actualResource))
                    .describedAs("Expect YAML match expected")
                    .isEqualTo(YAML_MAPPER.writeValueAsString(expected));
            // We add this assertion just in case the String-based YAML assertion above didn't fail
            // If this assertion fails then it means that (weirdly) the YAML strings are the same
            // but the resources were not .equals() => probably a bug in the resources .equals(Object)
            Assertions.fail();
        }
    }

    @NonNull
    private static Context<KafkaProxy> buildContext(Path testDir,
                                                    KafkaProxy kafkaProxy,
                                                    List<VirtualKafkaCluster> virtualKafkaClusters,
                                                    List<KafkaService> kafkaServiceRefs,
                                                    List<KafkaProxyIngress> ingresses)
            throws IOException {
        Answer<?> throwOnUnmockedInvocation = invocation -> {
            var stringifiedArgs = Arrays.stream(invocation.getArguments()).map(String::valueOf).collect(
                    Collectors.joining(", "));
            throw new RuntimeException("Unmocked method: " + invocation.getMethod() + "(" + stringifiedArgs + ")");
        };
        Context<KafkaProxy> context = mock(Context.class);

        var resourceContext = new DefaultManagedWorkflowAndDependentResourceContext(null, null, context);
        resourceContext.put(Crc32ChecksumGenerator.CHECKSUM_CONTEXT_KEY, new FixedChecksumGenerator(123654L));
        doReturn(resourceContext).when(context).managedWorkflowAndDependentResourceContext();

        Set<KafkaProtocolFilter> filterInstances = Set.copyOf(resourcesFromFiles(TestFiles.childFilesMatching(testDir,
                "in-" + HasMetadata.getKind(KafkaProtocolFilter.class) + "-*.yaml"), KafkaProtocolFilter.class));

        doReturn(filterInstances).when(context).getSecondaryResources(KafkaProtocolFilter.class);
        doReturn(Set.copyOf(virtualKafkaClusters)).when(context).getSecondaryResources(VirtualKafkaCluster.class);
        doReturn(Set.copyOf(kafkaServiceRefs)).when(context).getSecondaryResources(KafkaService.class);
        doReturn(Set.copyOf(ingresses)).when(context).getSecondaryResources(KafkaProxyIngress.class);

        new KafkaProxyReconciler(TEST_CLOCK, SecureConfigInterpolator.DEFAULT_INTERPOLATOR)
                .initContext(kafkaProxy, context);

        return context;
    }

    private static String fileName(Path testDir) {
        return testDir.getFileName().toString();
    }

    private static <T> T loadExpected(Path path, Class<T> type) {
        File file = path.toFile();
        try {
            return YAML_MAPPER.readValue(file, type);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Reading " + file + " as YAML for type " + type, e);
        }
    }

}
