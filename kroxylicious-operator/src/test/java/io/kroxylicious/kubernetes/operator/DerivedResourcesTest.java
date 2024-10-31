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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.BulkDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResource;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

public class DerivedResourcesTest {

    @FunctionalInterface
    interface TriFunction<X, Y, Z, R> {
        R apply(X x, Y y, Z z);
    }

    /**
     * Abstraction over KubernetesDependentResource and BulkDependentResource
     * @param <P> The primary type (KafkaProxy)
     * @param <R> The resource type of the dependent resource (e.g. Service)
     */
    interface DesiredFn<P extends HasMetadata, R extends HasMetadata> {
        /**
         * @return A map of expected file name (i.e. a classpath resource which contains the YAML for the expected resource)
         * to actual resource
         */
        Map<String, R> invokeDesired(P primary, Context<P> context);

        /**
         * @return The type of derived resource (e.g. Service.class)
         */
        Class<R> derivedResourceType();

    }

    /**
     * Specialization of {@link DesiredFn} for KubernetesDependentResource
     * (i.e. where the desired() method returns an R).
     */
    record SingletonDependentResourceDesiredFn<D extends KubernetesDependentResource<R, P>, P extends HasMetadata, R extends HasMetadata>(
                                                                                                                                          D dependentResource,
                                                                                                                                          TriFunction<D, P, Context<P>, R> fn)
            implements DesiredFn<P, R> {
        @Override
        public Map<String, R> invokeDesired(P primary, Context<P> context) {
            R resource = fn.apply(dependentResource, primary, context);
            return Map.of(filenameOfExpectedFile(derivedResourceType(), resource), resource);
        }

        @Override
        public Class<R> derivedResourceType() {
            return dependentResource.resourceType();
        }
    }

    /**
     * Specialization of {@link DesiredFn} for BulkDependentResource
     * (i.e. where the desired() method returns a Map<String, R>).
     */
    record BulkDependentResourceDesiredFn<D extends KubernetesDependentResource<R, P> & BulkDependentResource<R, P>, P extends HasMetadata, R extends HasMetadata>(
                                                                                                                                                                   D dependentResource,
                                                                                                                                                                   TriFunction<D, P, Context<P>, Map<String, R>> fn)
            implements DesiredFn<P, R> {
        @Override
        public Map<String, R> invokeDesired(P primary, Context<P> context) {
            return fn.apply(dependentResource, primary, context).entrySet().stream().collect(Collectors.toMap(
                    entry -> filenameOfExpectedFile(derivedResourceType(), entry.getValue()),
                    Map.Entry::getValue));
        }

        @Override
        public Class<R> derivedResourceType() {
            return dependentResource.resourceType();
        }

    }

    @NonNull
    private static <R extends HasMetadata> String filenameOfExpectedFile(Class<R> derivedResourceType, R resource) {
        return "out-" + derivedResourceType.getSimpleName() + "-" + resource.getMetadata().getName() + ".yaml";
    }

    @TestFactory
    Stream<DynamicContainer> dependentResourcesShouldEqual() throws IOException {
        var list = List.<DesiredFn<KafkaProxy, ?>> of(
                new SingletonDependentResourceDesiredFn<>(new ProxyConfigSecret(), ProxyConfigSecret::desired),
                new SingletonDependentResourceDesiredFn<>(new ProxyDeployment(), ProxyDeployment::desired),
                new BulkDependentResourceDesiredFn<>(new ClusterService(), ClusterService::desiredResources),
                new SingletonDependentResourceDesiredFn<>(new MetricsService(), MetricsService::desired));
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
                    Path kafkaProxyYaml = testDir.resolve("in-KafkaProxy.yaml");
                    KafkaProxy kafkaProxy = Util.kafkaProxyFromFile(kafkaProxyYaml);

                    List<DynamicTest> tests = new ArrayList<>();

                    var allPresentExpectedFilenames = filesInDir(testDir, "out-*").stream()
                            .map(DerivedResourcesTest::fileName)
                            .collect(Collectors.toCollection(HashSet::new));

                    var dr = list.stream().map(r -> r.invokeDesired(kafkaProxy, null)).toList();
                    var allReturnedExpectedFilenames = dr.stream().flatMap(m -> m.entrySet().stream()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    var collisions1 = dr.stream().flatMap(m -> m.entrySet().stream()).collect(Collectors.groupingBy(Map.Entry::getKey));
                    var collisions = collisions1.entrySet().stream().filter(entry -> entry.getValue().size() >= 2).map(entry -> entry.getKey()).toList();
                    tests.add(DynamicTest.dynamicTest("No Colliding Resources", () -> {
                        assertThat(collisions)
                                .describedAs("Distinct KubernetesDependentResources returned the same Kubernetes resource")
                                .isEmpty();
                    }));

                    // for (var r : list) {
                    // var dependentResource = r.invokeDesired(kafkaProxy, null);
                    // for (var expectedFile : dependentResource.keySet()) {
                    // var l = collisions.computeIfAbsent(expectedFile, k -> new ArrayList<>());
                    // l.add(r);
                    // }
                    //
                    // // assert that over all the DependentResources there is no file name collision
                    // for (var expectedFile : dependentResource.keySet()) {
                    // tests.add(DynamicTest.dynamicTest("No Colliding Resources", () -> {
                    // assertThat(allReturnedExpectedFilenames)
                    // .describedAs("Two different KubernetesDependentResources returned the same Kubernetes resource: " + expectedFile)
                    // .doesNotContainKey(expectedFile);
                    // }));
                    //
                    // }
                    // allReturnedExpectedFilenames.putAll(dependentResource);
                    // }
                    // collisions.entrySet().stream().filter(entry -> entry.)

                    // assert that the returned expected file all exist
                    var missingFiles = allReturnedExpectedFilenames.keySet().stream().filter(expectedEntry -> !Files.exists(testDir.resolve(expectedEntry))).toList();
                    tests.add(DynamicTest.dynamicTest("No missing expected YAMLs", () -> {
                        assertThat(missingFiles)
                                .describedAs("Expected resource YAML file does not exist")
                                .isEmpty();
                    }));

                    // assert that no other files exist
                    allPresentExpectedFilenames.removeAll(allReturnedExpectedFilenames.keySet());
                    tests.add(DynamicTest.dynamicTest("No Unexpected YAMLs", () -> {
                        assertThat(allPresentExpectedFilenames)
                                .describedAs(testDir + " contained expected resource YAML files which are not produced by any KubernetesDependentResources")
                                .isEmpty();
                    }));

                    for (var returnedEntry : allReturnedExpectedFilenames.entrySet()) {
                        // assert the contents of the returned resources match the expected resources
                        String expectedFilename = returnedEntry.getKey();
                        var dependentResource = returnedEntry.getValue();
                        tests.add(DynamicTest.dynamicTest("Contents match " + expectedFilename, () -> {
                            var expected = loadExpected(testDir, expectedFilename, dependentResource.getClass());
                            if (!expected.equals(dependentResource)) {
                                // Failing with a String-based assert makes it **much** easier to understand what the diffs are
                                // because we're comparing YAML strings, rather than the resources.toString()
                                // which is not normally YAML
                                assertThat(Util.YAML_MAPPER.writeValueAsString(dependentResource))
                                        .describedAs("Expect returned resource to match expected")
                                        .isEqualTo(Util.YAML_MAPPER.writeValueAsString(expected));
                                // We add this assertion just in case the String-based YAML assertion above didn't fail
                                // If this assertion fails then it means that (weirdly) the YAML strings are the same
                                // but the resources were not .equals() => probably a bug in the resources .equals(Object)
                                assertThat(false).isTrue();
                            }
                        }));
                    }

                    return DynamicContainer.dynamicContainer(testCase, tests);
                });

    }

    private static String fileName(Path testDir) {
        return testDir.getName(testDir.getNameCount() - 1).toString();
    }

    private static <R extends HasMetadata> R loadExpected(Path testDir, String expectedYamlFile, Class<R> derivedResourceType) {
        R expected;
        try {
            expected = Util.YAML_MAPPER.readValue(testDir.resolve(expectedYamlFile).toFile(), derivedResourceType);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return expected;
    }

}
