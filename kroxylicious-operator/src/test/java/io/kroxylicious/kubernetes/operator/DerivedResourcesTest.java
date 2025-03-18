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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.mockito.stubbing.Answer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.DefaultManagedWorkflowAndDependentResourceContext;
import io.javaoperatorsdk.operator.processing.dependent.BulkDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResource;


import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.config.RuntimeDecl;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.ProxyDeployment.KROXYLICIOUS_IMAGE_ENV_VAR;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class DerivedResourcesTest {

    static final YAMLMapper YAML_MAPPER = new YAMLMapper()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
            .enable(YAMLGenerator.Feature.LITERAL_BLOCK_STYLE);

    public static KafkaProxy kafkaProxyFromFile(Path path) {
        // TODO should validate against the CRD schema, because the DependentResource
        // should never see an invalid resource in production
        try {
            return YAML_MAPPER.readValue(path.toFile(), KafkaProxy.class);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static <T extends HasMetadata> List<T> resourcesFromFiles(Set<Path> paths, Class<T> valueType) {
        // TODO should validate against the CRD schema, because the DependentResource
        // should never see an invalid resource in production
        List<T> resources = paths.stream().map(path -> {
            try {
                return YAML_MAPPER.readValue(path.toFile(), valueType);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).sorted(Comparator.comparing(ResourcesUtil::name)).toList();
        long uniqueResources = resources.stream().map(s -> namespace(s) + ":" + name(s)).distinct().count();
        // sanity check that the identifiers are unique
        assertThat(uniqueResources)
                .overridingErrorMessage("unexpected number of unique resources from files: %s", paths)
                .isEqualTo(paths.size());
        return resources;
    }

    public static List<KafkaProxyIngress> kafkaProxyIngressesFromFiles(Set<Path> paths) {
        // TODO should validate against the CRD schema, because the DependentResource
        // should never see an invalid resource in production
        List<KafkaProxyIngress> ingresses = paths.stream().map(path -> {
            try {
                return YAML_MAPPER.readValue(path.toFile(), KafkaProxyIngress.class);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).sorted(Comparator.comparing(ResourcesUtil::name)).toList();
        long uniqueResources = ingresses.stream().map(ingress -> namespace(ingress) + ":" + name(ingress)).distinct().count();
        // sanity check that the identifiers are unique
        assertThat(uniqueResources).isEqualTo(paths.size());
        return ingresses;
    }

    public static RuntimeDecl configFromFile(Path path) {
        // TODO should validate against the Config schema, because the DependentResource
        // should never see an invalid config in production
        try {
            return YAML_MAPPER.readValue(path.toFile(), RuntimeDecl.class);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
                                                                                                                                          TriFunction<D, P, Context<P>, R> fn)
            implements DesiredFn<P, R> {
        @Override
        public Class<R> resourceType() {
            return dependentResource.resourceType();
        }

        @Override
        public Map<String, R> invokeDesired(P primary, Context<P> context) {
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
                new SingletonDependentResourceDesiredFn<>(new ProxyConfigConfigMap(), "ConfigMap", ProxyConfigConfigMap::desired),
                new SingletonDependentResourceDesiredFn<>(new ProxyDeployment(), "Deployment", ProxyDeployment::desired),
                new BulkDependentResourceDesiredFn<>(new ClusterService(), "Service", ClusterService::desiredResources));
        return dependentResourcesShouldEqual(list);
    }

    static List<Path> filesInDir(Path dir, Pattern pattern) {
        var result = new ArrayList<Path>();
        try (var expected = Files.newDirectoryStream(dir, path -> pattern.matcher(path.getFileName().toString()).matches())) {
            for (Path f : expected) {
                result.add(f);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return result;
    }

    Stream<DynamicContainer> dependentResourcesShouldEqual(List<DesiredFn<KafkaProxy, ?>> list) {
        var dir = Path.of("target", "test-classes", DerivedResourcesTest.class.getSimpleName());
        return filesInDir(dir, Pattern.compile(".*")).stream()
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

    record ConditionStruct(Condition.Type type,
                           String cluster,
                           String status,
                           String reason,
                           String message) {

    }

    @NonNull
    private static List<DynamicTest> testsForDir(List<DesiredFn<KafkaProxy, ?>> dependentResources,
                                                 Path testDir)
            throws IOException {
        try {
            var unusedFiles = childFilesMatching(testDir, "*");
            String inFileName = "in-KafkaProxy.yaml";
            Path input = testDir.resolve(inFileName);
            KafkaProxy kafkaProxy = kafkaProxyFromFile(input);
            List<VirtualKafkaCluster> virtualKafkaClusters = resourcesFromFiles(childFilesMatching(testDir, "in-VirtualKafkaCluster-*"), VirtualKafkaCluster.class);
            List<KafkaService> kafkaServiceRefs = resourcesFromFiles(childFilesMatching(testDir, "in-KafkaService-*"), KafkaService.class);
            assertMinimalMetadata(kafkaProxy.getMetadata(), inFileName);
            List<KafkaProxyIngress> ingresses = kafkaProxyIngressesFromFiles(childFilesMatching(testDir, "in-KafkaProxyIngress-*"));

            unusedFiles.remove(input);
            unusedFiles.removeAll(childFilesMatching(testDir, "in-*"));

            Context<KafkaProxy> context;
            try {
                context = buildContext(testDir, virtualKafkaClusters, kafkaServiceRefs, ingresses);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            List<DynamicTest> tests = new ArrayList<>();

            var dr = dependentResources.stream()
                    .flatMap(r -> r.invokeDesired(kafkaProxy, context).values().stream().map(x -> Map.entry(r.resourceType(), x)))
                    .collect(Collectors.groupingBy(Map.Entry::getKey))
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                            e -> e.getValue().stream().map(Map.Entry::getValue).collect(Collectors.toCollection(() -> new TreeSet<>(
                                    Comparator.comparing(ResourcesUtil::name))))));
            for (var entry : dr.entrySet()) {
                var resourceType = entry.getKey();
                var actualResources = entry.getValue();
                for (var actualResource : actualResources) {
                    String kind = resourceType.getSimpleName();
                    String name = name(actualResource);
                    var expectedFile = testDir.resolve("out-" + kind + "-" + name + ".yaml");
                    tests.add(DynamicTest.dynamicTest(kind + " '" + name + "' should have the same content as " + testDir.relativize(expectedFile),
                            () -> {
                                assertThat(Files.exists(expectedFile)).isTrue();
                                var expected = loadExpected(expectedFile, resourceType);
                                assertSameYaml(actualResource, expected);
                                unusedFiles.remove(expectedFile);
                            }));
                }
                for (var cluster : virtualKafkaClusters) {
                    ClusterCondition actualClusterCondition = SharedKafkaProxyContext.clusterCondition(context, cluster);
                    if (actualClusterCondition.type() == Condition.Type.Accepted && actualClusterCondition.status().equals(Condition.Status.TRUE)) {
                        continue;
                    }
                    else {
                        var expectedFile = testDir.resolve("cond-" + actualClusterCondition.type() + "-" + actualClusterCondition.cluster() + ".yaml");
                        tests.add(DynamicTest.dynamicTest(
                                "Condition " + actualClusterCondition.type() + " for cluster " + actualClusterCondition.cluster() + " matches contents of expected file "
                                        + expectedFile,
                                () -> {
                                    var expected = loadExpected(expectedFile, ClusterCondition.class);
                                    assertSameYaml(actualClusterCondition, expected);
                                    unusedFiles.remove(expectedFile);
                                }));
                    }
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
    private static HashSet<Path> childFilesMatching(
                                                    Path testDir,
                                                    String glob)
            throws IOException {
        return StreamSupport.stream(Files.newDirectoryStream(testDir, glob).spliterator(), false)
                .collect(Collectors.toCollection(HashSet::new));
    }

    @NonNull
    private static Context<KafkaProxy> buildContext(Path testDir, List<VirtualKafkaCluster> virtualKafkaClusters, List<KafkaService> kafkaServiceRefs,
                                                    List<KafkaProxyIngress> ingresses)
            throws IOException {
        Answer<?> throwOnUnmockedInvocation = invocation -> {
            var stringifiedArgs = Arrays.stream(invocation.getArguments()).map(String::valueOf).collect(
                    Collectors.joining(", "));
            throw new RuntimeException("Unmocked method: " + invocation.getMethod() + "(" + stringifiedArgs + ")");
        };
        Context<KafkaProxy> context = mock(Context.class, throwOnUnmockedInvocation);

        var resourceContext = new DefaultManagedWorkflowAndDependentResourceContext(null, null, context);

        doReturn(resourceContext).when(context).managedWorkflowAndDependentResourceContext();

        var runtimeDecl = OperatorMain.runtimeDecl();
        Set<GenericKubernetesResource> filterInstances = new HashSet<>();
        for (var filterApi : runtimeDecl.filterApis()) {
            String fileName = "in-" + filterApi.kind() + "-*.yaml";
            try (var dirStream = Files.newDirectoryStream(testDir, fileName)) {
                for (Path p : dirStream) {
                    GenericKubernetesResource resource = YAML_MAPPER.readValue(p.toFile(), GenericKubernetesResource.class);
                    assertMinimalMetadata(resource.getMetadata(), fileName);
                    filterInstances.add(resource);
                }
            }
        }
        doReturn(filterInstances).when(context).getSecondaryResources(GenericKubernetesResource.class);
        doReturn(Set.copyOf(virtualKafkaClusters)).when(context).getSecondaryResources(VirtualKafkaCluster.class);
        doReturn(Set.copyOf(kafkaServiceRefs)).when(context).getSecondaryResources(KafkaService.class);
        doReturn(Set.copyOf(ingresses)).when(context).getSecondaryResources(KafkaProxyIngress.class);
        SharedKafkaProxyContext.runtimeDecl(context, runtimeDecl);
        return context;
    }

    record ErrorStruct(String type, String message) {}

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
