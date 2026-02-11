/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.helm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.opentest4j.AssertionFailedError;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Utility class for executing Helm CLI commands and parsing Kubernetes YAML manifests.
 */
public class HelmUtils {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
    private static final Path HELM_CHART_DIR = getHelmChartDirectory();
    private static final Path TEST_VALUES_FILE = getTestValuesFile();
    private static final int PROCESS_TIMEOUT_SECONDS = 30;

    private static Path getHelmChartDirectory() {
        String chartDirProperty = System.getProperty("helm.chart.directory");
        if (chartDirProperty != null) {
            return Paths.get(chartDirProperty);
        }
        return Paths.get("helm/kroxylicious-benchmark").toAbsolutePath();
    }

    private static Path getTestValuesFile() {
        // Look for test-values.yaml in test resources
        String testValuesProperty = System.getProperty("helm.test.values");
        if (testValuesProperty != null) {
            return Paths.get(testValuesProperty);
        }
        return Paths.get("src/test/resources/test-values.yaml").toAbsolutePath();
    }

    /**
     * Runs helm lint on the chart.
     *
     * @return lint output
     */
    public static String lint() {
        List<String> command = List.of("helm", "lint", HELM_CHART_DIR.toString());
        return executeCommand(command);
    }

    /**
     * Renders Helm templates and returns the raw YAML output.
     *
     * @return rendered template YAML
     * @throws IOException if helm command fails
     */
    public static String renderTemplate() throws IOException {
        return renderTemplate(Map.of());
    }

    /**
     * Renders Helm templates with custom values and returns the raw YAML output.
     * Uses test-values.yaml as base, with optional --set overrides.
     *
     * @param setValues Map of key-value pairs to pass as --set arguments
     * @return rendered template YAML
     */
    public static String renderTemplate(Map<String, String> setValues) {
        List<String> command = new ArrayList<>();
        command.add("helm");
        command.add("template");
        command.add("test-release");
        command.add(HELM_CHART_DIR.toString());
        command.add("-f");
        command.add(TEST_VALUES_FILE.toString());

        for (Map.Entry<String, String> entry : setValues.entrySet()) {
            command.add("--set");
            command.add(entry.getKey() + "=" + entry.getValue());
        }

        return executeCommand(command);
    }

    /**
     * Validates that Helm is installed and available.
     *
     * @return true if Helm is available
     */
    public static boolean isHelmAvailable() {
        try {
            executeCommand(List.of("helm", "version"));
            return true;
        }
        catch (AssertionFailedError e) {
            return false;
        }
    }

    /**
     * Executes a command and returns its output.
     *
     * @param command Command and arguments to execute
     * @return STD_OUT from executing the command
     */
    private static String executeCommand(List<String> command) {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);

        try {
            Process process = pb.start();
            String output;

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                output = reader.lines().collect(Collectors.joining("\n"));
            }

            boolean finished = process.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                fail("Command timed out (after %s): %s", Duration.ofSeconds(PROCESS_TIMEOUT_SECONDS), String.join(" ", command));
            }

            int exitCode = process.exitValue();
            assertThat(exitCode).as("Command failed: %s \nOutput:\n %s", String.join(" ", command), output).isZero();
            return output;
        }
        catch (IOException ioe) {
            fail("Failed to execute command: %s" + String.join(" ", command), ioe);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Failed to execute command: %s" + String.join(" ", command), e);
        }
        return null;
    }

    /**
     * Parses YAML output containing multiple Kubernetes resources (separated by ---).
     * <p>
     * Note: We manually split by "---" rather than using Jackson's MappingIterator because
     * Helm templates include source comments (e.g., "# Source: chart/templates/file.yaml")
     * and license headers that must be filtered out before parsing.
     * </p>
     *
     * @param yaml YAML string potentially containing multiple documents
     * @return List of parsed resources as Maps
     * @throws IOException if YAML parsing fails
     */
    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> parseKubernetesManifests(String yaml) throws IOException {
        List<Map<String, Object>> resources = new ArrayList<>();

        // Split by YAML document separator
        String[] documents = yaml.split("---");

        for (String doc : documents) {
            String cleaned = removeCommentLines(doc);

            if (cleaned.isEmpty()) {
                continue; // Skip empty documents
            }

            try {
                Map<String, Object> resource = YAML_MAPPER.readValue(cleaned, Map.class);
                if (resource != null && !resource.isEmpty()) {
                    resources.add(resource);
                }
            }
            catch (IOException e) {
                throw new IOException("Failed to parse YAML document: " + e.getMessage() +
                        "\nDocument content (first 200 chars): " + cleaned.substring(0, Math.min(200, cleaned.length())), e);
            }
        }

        return resources;
    }

    /**
     * Removes comment lines from YAML content.
     * Filters out Helm source comments and license headers.
     *
     * @param yaml YAML content with potential comments
     * @return YAML content with comments removed
     */
    private static String removeCommentLines(String yaml) {
        return yaml.lines()
                .filter(line -> !line.trim().startsWith("#"))
                .collect(java.util.stream.Collectors.joining("\n"))
                .trim();
    }

    /**
     * Finds a resource by kind and name in a list of resources.
     *
     * @param resources List of Kubernetes resources
     * @param kind Resource kind (e.g., "StatefulSet", "ConfigMap")
     * @param name Resource name
     * @return The matching resource, or null if not found
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> findResource(List<Map<String, Object>> resources, String kind, String name) {
        return resources.stream()
                .filter(r -> kind.equals(r.get("kind")))
                .filter(r -> {
                    Map<String, Object> metadata = (Map<String, Object>) r.get("metadata");
                    return metadata != null && name.equals(metadata.get("name"));
                })
                .findFirst()
                .orElse(null);
    }

    /**
     * Parses YAML output into typed Kubernetes resources using Fabric8 models.
     * <p>
     * This provides type-safe access to Kubernetes resources, eliminating raw Map casting.
     * Resources are parsed as GenericKubernetesResource which provides type-safe access to
     * metadata and keeps spec as a Map for flexibility.
     * </p>
     *
     * @param yaml YAML string potentially containing multiple documents
     * @return List of parsed Kubernetes resources
     * @throws IOException if YAML parsing fails
     */
    public static List<GenericKubernetesResource> parseKubernetesResourcesTyped(String yaml) throws IOException {
        List<GenericKubernetesResource> resources = new ArrayList<>();

        // Split by YAML document separator
        String[] documents = yaml.split("---");

        for (String doc : documents) {
            String cleaned = removeCommentLines(doc);

            if (cleaned.isEmpty()) {
                continue;
            }

            try {
                GenericKubernetesResource resource = YAML_MAPPER.readValue(cleaned, GenericKubernetesResource.class);
                if (resource != null && resource.getKind() != null) {
                    resources.add(resource);
                }
            }
            catch (IOException e) {
                throw new IOException("Failed to parse YAML document: " + e.getMessage() +
                        "\nDocument content (first 200 chars): " + cleaned.substring(0, Math.min(200, cleaned.length())), e);
            }
        }

        return resources;
    }

    /**
     * Finds a typed resource by kind and name.
     *
     * @param resources List of Kubernetes resources
     * @param kind Resource kind (e.g., "Kafka", "Service")
     * @param name Resource name
     * @return The matching resource, or null if not found
     */
    public static GenericKubernetesResource findResourceTyped(List<GenericKubernetesResource> resources, String kind, String name) {
        return resources.stream()
                .filter(r -> kind.equals(r.getKind()))
                .filter(r -> r.getMetadata() != null && name.equals(r.getMetadata().getName()))
                .findFirst()
                .orElse(null);
    }

    /**
     * Gets the pod template spec from a Deployment.
     *
     * @param deployment Deployment resource
     * @return Pod template spec as a Map
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> getPodTemplateSpec(GenericKubernetesResource deployment) {
        assertThat(deployment).as("Deployment resource should not be null").isNotNull();
        assertThat(deployment.getKind()).as("Resource should be a Deployment").isEqualTo("Deployment");

        Map<String, Object> spec = deployment.get("spec");
        assertThat(spec)
                .as("Deployment '%s' should have spec", deployment.getMetadata().getName())
                .isNotNull();

        Map<String, Object> template = (Map<String, Object>) spec.get("template");
        assertThat(template)
                .as("Deployment '%s' should have pod template", deployment.getMetadata().getName())
                .isNotNull();

        Map<String, Object> podSpec = (Map<String, Object>) template.get("spec");
        assertThat(podSpec)
                .as("Deployment '%s' template should have spec", deployment.getMetadata().getName())
                .isNotNull();

        return podSpec;
    }

    /**
     * Gets the value of an environment variable from a Pod's first container.
     * Works with both Pod and Deployment resources (extracts pod template from Deployment).
     * Uses assertions to validate structure for clear test failure messages.
     *
     * @param resource Pod or Deployment resource
     * @param envVarName Name of the environment variable
     * @return Environment variable value
     */
    @SuppressWarnings("unchecked")
    public static String getPodEnvVar(GenericKubernetesResource resource, String envVarName) {
        assertThat(resource).as("Resource should not be null").isNotNull();

        Map<String, Object> spec;
        if ("Deployment".equals(resource.getKind())) {
            spec = getPodTemplateSpec(resource);
        }
        else if ("Pod".equals(resource.getKind())) {
            spec = resource.get("spec");
            assertThat(spec)
                    .as("Pod '%s' should have spec", resource.getMetadata().getName())
                    .isNotNull();
        }
        else {
            fail("Resource '%s' must be a Pod or Deployment, but was %s", resource.getMetadata().getName(), resource.getKind());
            return null; // unreachable
        }

        List<Map<String, Object>> containers = (List<Map<String, Object>>) spec.get("containers");
        assertThat(containers)
                .as("Resource '%s' should have containers", resource.getMetadata().getName())
                .isNotNull()
                .isNotEmpty();

        Map<String, Object> container = containers.get(0);
        List<Map<String, Object>> env = (List<Map<String, Object>>) container.get("env");
        assertThat(env)
                .as("Resource '%s' container should have env section", resource.getMetadata().getName())
                .isNotNull();

        return env.stream()
                .filter(e -> envVarName.equals(e.get("name")))
                .map(e -> (String) e.get("value"))
                .findFirst()
                .orElseGet(() -> fail("Resource '%s' does not have environment variable '%s'", resource.getMetadata().getName(), envVarName));
    }

}
