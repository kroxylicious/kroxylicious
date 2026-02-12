# Testing Infrastructure

This document describes the testing infrastructure added to the Kroxylicious OpenMessaging Benchmarks project.

## Overview

The testing infrastructure ensures that the Helm chart works correctly and continues to work as changes are made. It includes:

1. **Template Rendering Tests** - Validate Helm templates render correctly
2. **Values Validation Tests** - Validate configuration combinations
3. **Helm Lint Tests** - Validate chart structure and syntax
4. **Continuous Integration** - Automated testing on every commit

## Test Statistics

- **Total Tests**: Varies (parameterized tests run multiple times)
- **Test Classes**: 2
- **Test Coverage**:
  - Template rendering scenarios: Validates Strimzi Kafka CRD structure
  - Helm lint validation: Validates chart syntax

## Files Added

### Maven Configuration
- `pom.xml` - Maven project configuration with test dependencies

### Test Source Code
- `src/test/java/io/kroxylicious/benchmarks/helm/HelmUtils.java`
  - Utility class for executing Helm CLI commands
  - YAML parsing and Kubernetes resource handling
  - Resource finding and validation helpers

- `src/test/java/io/kroxylicious/benchmarks/helm/HelmTemplateRenderingTest.java`
  - Tests for template rendering with various configurations
  - Parameterized tests for different broker counts
  - Validation of Strimzi Kafka custom resource structure
  - Security context validation

- `src/test/java/io/kroxylicious/benchmarks/helm/HelmLintTest.java`
  - Helm lint validation
  - Ensures chart passes linting with no warnings or errors

### CI/CD Configuration
- `.github/workflows/ci.yml`
  - GitHub Actions workflow for automated testing
  - Runs on push and pull requests
  - Jobs: helm-lint, test, verify

## Running Tests

### Prerequisites

- Java 17+
- Maven 3.6+
- Helm 3.0+
- **Note**: Tests do not require Strimzi operator to be installed (they only validate template rendering)

### Commands

```bash
# Run all tests
mvn clean test

# Run specific test class
mvn test -Dtest=HelmTemplateRenderingTest
mvn test -Dtest=HelmLintTest

# Run full verification (includes all quality checks)
mvn clean verify
```

## Test Examples

### Template Rendering Test Example

```java
@ParameterizedTest
@ValueSource(ints = { 1, 3, 5 })
void shouldRenderWithConfigurableReplicaCount(int replicas) throws IOException {
    String yaml = HelmUtils.renderTemplate(Map.of("kafka.replicas", String.valueOf(replicas)));
    List<Map<String, Object>> resources = HelmUtils.parseKubernetesManifests(yaml);
    Map<String, Object> kafka = HelmUtils.findResource(resources, "Kafka", "kafka");

    assertThat(kafka).isNotNull();
    Map<String, Object> spec = (Map<String, Object>) kafka.get("spec");
    Map<String, Object> kafkaSpec = (Map<String, Object>) spec.get("kafka");
    assertThat(kafkaSpec.get("replicas"))
            .as("Kafka CR should have %d replicas", replicas)
            .isEqualTo(replicas);
}
```

## CI/CD Pipeline

The GitHub Actions workflow runs tests to validate:

1. **helm-lint**: Validates Helm chart structure and syntax
2. **test**: Validates Strimzi Kafka CRD renders correctly with various configurations
3. **verify**: Full verification including all quality checks

All jobs must pass before a PR can be merged.

**Note**: CI tests do not deploy a Kafka cluster - they only validate that Helm templates render valid Kubernetes manifests.

## Design Decisions

### Why Java + Maven?

- **Consistency**: Matches the main Kroxylicious project's test infrastructure
- **Familiarity**: Kroxylicious contributors already know Java/Maven/JUnit
- **Reusability**: Can leverage existing test patterns and utilities
- **Integration**: Easy to integrate with existing CI/CD

### Why Shell Out to Helm CLI?

- **Simplicity**: No need to parse HCL or implement Helm's rendering logic
- **Accuracy**: Uses the exact same rendering engine as production
- **Maintenance**: Automatically stays compatible with Helm updates

### Strimzi Integration

The Helm chart uses **Strimzi** for Kafka deployment:
- Templates render a `Kafka` custom resource (kind: Kafka, apiVersion: kafka.strimzi.io/v1beta2)
- Tests validate the Kafka CR structure, not StatefulSets directly
- Strimzi operator handles creating StatefulSets, Services, and ConfigMaps at runtime
- Tests only validate template rendering - no actual Kubernetes deployment happens

### Test Organization

Tests are organized by concern:
- **Template Rendering** - Validates correct Kafka CR generation
- **Lint** - Validates chart structure

This separation makes tests easier to understand and maintain.

## Extending Tests

### Adding a New Template Rendering Test

1. Add test method to `HelmTemplateRenderingTest`
2. Use `HelmUtils.renderTemplate()` to generate resources
3. Use `HelmUtils.findResource()` or `findResourcesByKind()` to locate resources
4. Use AssertJ assertions to validate resource properties

### Testing Strimzi Kafka CR Structure

When testing Kafka CR properties, navigate the nested structure:
```java
Map<String, Object> kafka = findResource(resources, "Kafka", "kafka");
Map<String, Object> spec = kafka.get("spec");
Map<String, Object> kafkaSpec = spec.get("kafka");
// kafkaSpec.get("replicas"), kafkaSpec.get("version"), etc.
```

## Best Practices

1. **Use Descriptive Test Names**: Test method names should clearly describe what is being tested
2. **Use AssertJ Fluent Assertions**: They provide better error messages
3. **Test Edge Cases**: Single broker, many brokers, minimal configs, maximal configs
4. **Keep Tests Fast**: Template rendering tests should complete in under 1 second each
5. **Document Assumptions**: Use `.as()` to document what each assertion validates

## Troubleshooting

### Tests Fail with "Helm is not installed"

Ensure Helm is installed and available in PATH:
```bash
helm version
```

### Tests Fail with YAML Parsing Errors

The YAML parser filters out comments. If templates change significantly, check `HelmUtils.parseKubernetesManifests()`.

### Tests Fail in CI but Pass Locally

Check Java and Helm versions match between local and CI environments.

## Future Enhancements

Potential future testing improvements:

1. **Integration Tests**: Deploy chart to Kind/Minikube with Strimzi operator and verify actual Kafka cluster startup
2. **Performance Tests**: Measure Helm rendering performance
3. **Schema Validation**: JSON Schema validation for values.yaml
4. **Snapshot Testing**: Compare rendered Kafka CR against golden files
5. **Strimzi Version Testing**: Test with different Strimzi operator versions

## License

Apache License 2.0
