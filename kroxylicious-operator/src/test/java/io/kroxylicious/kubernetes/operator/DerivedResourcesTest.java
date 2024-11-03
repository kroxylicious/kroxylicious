/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DefaultContext;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.processing.Controller;
import io.javaoperatorsdk.operator.processing.dependent.BulkDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResource;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
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

    @FunctionalInterface
    interface TriFunction<X, Y, Z, R> {
        R apply(X x, Y y, Z z);
    }

    record ResourceOrError<R extends HasMetadata>(
                                                  @Nullable R resource,
                                                  @Nullable Exception error,
                                                  @NonNull String kind) {
        ResourceOrError {
            if ((resource != null) == (error != null)) {
                throw new IllegalArgumentException();
            }
            Objects.requireNonNull(kind);
        }

        public void withResult(ThrowingConsumer<R> resourceFn,
                               ThrowingConsumer<Exception> errorFn) {
            if (resource != null) {
                resourceFn.accept(resource);
            }
            if (error != null) {
                errorFn.accept(error);
            }
        }

        static <R extends HasMetadata> ResourceOrError<R> resource(R resource) {
            return new ResourceOrError<>(resource, null, resource.getKind());
        }

        static <R extends HasMetadata> ResourceOrError<R> error(Exception error, String kind) {
            return new ResourceOrError<>(null, error, kind);
        }

        public Path filenameOfExpectedFile() {
            if (resource != null) {
                return Path.of("out-" + resource.getKind() + "-" + resource.getMetadata().getName() + ".yaml");
            }
            return Path.of("error-" + kind + /* "-" + resource.getMetadata().getName() + */".yaml");
        }
    }

    /**
     * Abstraction for invoking the desired() method on a KubernetesDependentResource and BulkDependentResource.
     * This is needed because they have different return types ({@code R} vs {@code Map<R>}).
     * @param <P> The primary type (KafkaProxy)
     * @param <R> The resource type of the dependent resource (e.g. Service)
     */
    interface DesiredFn<P extends HasMetadata, R extends HasMetadata> {
        /**
         * @return A map of expected file name (i.e. a classpath resource which contains the YAML for the expected resource)
         * to actual resource
         */
        Map<Path, ResourceOrError<R>> invokeDesired(P primary, Context<P> context);

    }

    /**
     * Specialization of {@link DesiredFn} for KubernetesDependentResource
     * (i.e. where the desired() method returns an R).
     */
    // formatter=off
    record SingletonDependentResourceDesiredFn<
            D extends KubernetesDependentResource<R, P>,
            P extends HasMetadata,
            R extends HasMetadata>
            (
                D dependentResource,
                String dependentResourceKind,
                TriFunction<D, P, Context<P>, R> fn
            )
            implements DesiredFn<P, R> {
        // formatter=on
        @Override
        public Map<Path, ResourceOrError<R>> invokeDesired(P primary, Context<P> context) {
            ResourceOrError<R> resource;
            try {
                resource = ResourceOrError.resource(fn.apply(dependentResource, primary, context));
            }
            catch (Exception e) {
                resource = ResourceOrError.error(e, dependentResourceKind);
            }
            return Map.of(resource.filenameOfExpectedFile(), resource);

        }
    }

    /**
     * Specialization of {@link DesiredFn} for BulkDependentResource
     * (i.e. where the desired() method returns a Map<String, R>).
     */
    // formatter=off
    record BulkDependentResourceDesiredFn<
            D extends KubernetesDependentResource<R, P> & BulkDependentResource<R, P>,
            P extends HasMetadata,
            R extends HasMetadata>
            (
                D dependentResource,
                String dependentResourceKind,
                TriFunction<D, P, Context<P>, Map<String, R>> fn
            )
            implements DesiredFn<P, R> {
        // formatter=on
        @Override
        public Map<Path, ResourceOrError<R>> invokeDesired(P primary, Context<P> context) {
            try {
                return fn.apply(dependentResource, primary, context).entrySet().stream().collect(Collectors.toMap(
                        entry -> ResourceOrError.resource(entry.getValue()).filenameOfExpectedFile(),
                        entry -> ResourceOrError.resource(entry.getValue())));
            }
            catch (Exception e) {
                ResourceOrError<R> error = ResourceOrError.error(e, dependentResourceKind);
                return Map.of(error.filenameOfExpectedFile(), error);
            }
        }
    }

    @TestFactory
    Stream<DynamicContainer> dependentResourcesShouldEqual() {
        var list = List.<DesiredFn<KafkaProxy, ?>> of(
                new SingletonDependentResourceDesiredFn<>(new ProxyConfigSecret(), "Secret", ProxyConfigSecret::desired),
                new SingletonDependentResourceDesiredFn<>(new ProxyDeployment(), "Deployment", ProxyDeployment::desired),
                new BulkDependentResourceDesiredFn<>(new ClusterService(), "Service", ClusterService::desiredResources),
                new SingletonDependentResourceDesiredFn<>(new MetricsService(), "Service", MetricsService::desired));
        return dependentResourcesShouldEqual(list);
    }

    static List<Path> filesInDir(Path dir, String glob) {
        var result = new ArrayList<Path>();
        try (var expected = Files.newDirectoryStream(dir, glob)) {
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
        var dir = Path.of("src", "test", "resources", DerivedResourcesTest.class.getSimpleName());
        return filesInDir(dir, "*").stream()
                .map(testDir -> {
                    String testCase = fileName(testDir);
                    List<DynamicTest> tests = testsForDir(list, testDir);
                    return DynamicContainer.dynamicContainer(testCase, tests);
                });

    }

    @NonNull
    private static List<DynamicTest> testsForDir(List<DesiredFn<KafkaProxy, ?>> list, Path testDir) {
        Path kafkaProxyYaml = testDir.resolve("in-KafkaProxy.yaml");
        KafkaProxy kafkaProxy = kafkaProxyFromFile(kafkaProxyYaml);

        List<DynamicTest> tests = new ArrayList<>();

        var allPresentExpectedFilenames = filesInDir(testDir, "out-*").stream()
                .map(testDir::relativize)
                .collect(Collectors.toCollection(HashSet::new));

        var dr = list.stream().map(r -> r.invokeDesired(kafkaProxy, new DefaultContext<KafkaProxy>(
                mock(RetryInfo.class), mock(Controller.class), kafkaProxy))).toList();
        var allReturnedExpectedFilenames = dr.stream().flatMap(m -> m.entrySet().stream()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        var groupedByFile = dr.stream().flatMap(m -> m.entrySet().stream()).collect(Collectors.groupingBy(Map.Entry::getKey));
        var collisions = groupedByFile.entrySet().stream().filter(entry -> entry.getValue().size() >= 2).map(Map.Entry::getKey).toList();
        tests.add(DynamicTest.dynamicTest("Dep resources return distinct resources", () -> {
            assertThat(collisions)
                    .describedAs("Distinct KubernetesDependentResources returned the same Kubernetes resource")
                    .isEmpty();
        }));

        // assert that the returned expected file all exist
        var missingFiles = allReturnedExpectedFilenames.keySet().stream().filter(expectedEntry -> !Files.exists(testDir.resolve(expectedEntry))).toList();
        tests.add(DynamicTest.dynamicTest("Each resource returned from a dep resource has an expected YAML", () -> {
            assertThat(missingFiles)
                    .describedAs("Expected resource YAML file(s) do not exist")
                    .isEmpty();
        }));

        // assert that no other files exist
        allPresentExpectedFilenames.removeAll(allReturnedExpectedFilenames.keySet());
        tests.add(DynamicTest.dynamicTest("All expected YAMLs correspond to a resource returned from a dep resource", () -> {
            assertThat(allPresentExpectedFilenames)
                    .describedAs(testDir + " contained expected resource YAML files which are not produced by any KubernetesDependentResources")
                    .isEmpty();
        }));

        for (var returnedEntry : allReturnedExpectedFilenames.entrySet()) {
            // assert the contents of the returned resources match the expected resources
            Path expectedFilename = returnedEntry.getKey();
            var resourceOrError = returnedEntry.getValue();
            tests.add(DynamicTest.dynamicTest("Contents match " + expectedFilename, () -> {
                resourceOrError.withResult(
                        resource -> {
                            var expected = loadExpected(testDir, expectedFilename, resource.getClass());
                            if (!expected.equals(resource)) {
                                // Failing with a String-based assert makes it **much** easier to understand what the diffs are
                                // because we're comparing YAML strings, rather than the resources.toString()
                                // which is not normally YAML
                                assertThat(YAML_MAPPER.writeValueAsString(resource))
                                        .describedAs("Expect returned resource to match expected")
                                        .isEqualTo(YAML_MAPPER.writeValueAsString(expected));
                                // We add this assertion just in case the String-based YAML assertion above didn't fail
                                // If this assertion fails then it means that (weirdly) the YAML strings are the same
                                // but the resources were not .equals() => probably a bug in the resources .equals(Object)
                                assertThat(false).isTrue();
                            }
                        },
                        error -> {
                            var errorStruct = YAML_MAPPER.readValue(testDir.resolve(resourceOrError.filenameOfExpectedFile()).toFile(), ErrorStruct.class);
                            var expectedMsg = new String(Files.readAllBytes(testDir.resolve(resourceOrError.filenameOfExpectedFile())));
                            assertThat(error.getClass().getName()).isEqualTo(errorStruct.type());
                            assertThat(error.getMessage()).isEqualTo(errorStruct.message());
                        });
            }));
        }
        return tests;
    }

    static record ErrorStruct(String type, String message) {}

    private static String fileName(Path testDir) {
        return testDir.getName(testDir.getNameCount() - 1).toString();
    }

    private static <R extends HasMetadata> R loadExpected(Path testDir, Path expectedYamlFile, Class<R> derivedResourceType) {
        R expected;
        try {
            expected = YAML_MAPPER.readValue(testDir.resolve(expectedYamlFile).toFile(), derivedResourceType);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return expected;
    }

}
