# Kroxylicious Migrations

The `kroxylicious-migrations` module provides automated refactoring recipes—powered primarily by [OpenRewrite](https://docs.openrewrite.org/)—to help downstream filter developers seamlessly update their projects when Kroxylicious introduces breaking API changes, package relocations, or deprecations.

---

## Module Architecture

Migration recipes live inside `src/main/resources/META-INF/rewrite/` as declarative YAML specifications. Each minor release requiring a migration receives its own versioned file, alongside an overarching aggregator file.

```text
kroxylicious-migrations/
└── src/
    └── main/
        └── resources/
            └── META-INF/
                └── rewrite/
                    ├── kroxylicious.yml  # Aggregator: UpgradeToLatest
                    ├── v0_24.yml         # 0.24.0 recipes (e.g., MigrateTo0_24)
                    └── v1_0.yml          # 1.0.0 recipes

```

### Recipe Design Guidelines

* **Atomic Recipes (`Use...`):** Focus on a single structural change (e.g., `io.kroxylicious.migrations.v0_24.UseKroxyliciousKafkaTypes`).
* **Version Aggregators (`MigrateTo...`):** Combines all atomic recipes for a specific release (e.g., `io.kroxylicious.migrations.v0_24.MigrateTo0_24`).
* **Latest Aggregator (`UpgradeToLatest`):** Combines all historical version aggregators in `kroxylicious.yml` so downstream projects can jump across multiple minor versions in one step.

---

## Downstream Execution (Filter Developers)

Downstream filter projects do not need to modify their `pom.xml` to execute published migrations. They can run OpenRewrite directly from the command line against published Maven Central artifacts:

**Apply a Specific Release Migration:**

```bash
mvn org.openrewrite.maven:rewrite-maven-plugin:run \
  -Drewrite.recipeArtifactCoordinates=io.kroxylicious:kroxylicious-migrations:<RELEASE_VERSION> \
  -Drewrite.activeRecipes=io.kroxylicious.migrations.v0_24.MigrateTo0_24

```

**Upgrade Across Multiple Releases to Latest:**

```bash
mvn org.openrewrite.maven:rewrite-maven-plugin:run \
  -Drewrite.recipeArtifactCoordinates=io.kroxylicious:kroxylicious-migrations:<RELEASE_VERSION> \
  -Drewrite.activeRecipes=io.kroxylicious.migrations.UpgradeToLatest

```

---

## In-Tree Execution (Core Contributors)

When developing or validating recipes against the local Kroxylicious repository, pass the absolute path using `$(pwd)` to bypass local artifact installation (`mvn install`):

> NOTE: the `java.version` is required in order to avoid a maven property conflict between Rewrite and properties used by Kroxylicious's own POMs.

**Preview Changes (Dry Run):**

```bash
mvn -Djava.version=21 org.openrewrite.maven:rewrite-maven-plugin:dryRun \
  -Drewrite.configLocation=$(pwd)/kroxylicious-proxy-core/kroxylicious-migrations/src/main/resources/META-INF/rewrite/v0_24.yml \
  -Drewrite.activeRecipes=io.kroxylicious.migrations.v0_24.MigrateTo0_24 \
  -Drewrite.exclusions="**/pom.xml,changelog/unreleased/**"

```

**Apply Changes In-Place:**

```bash
mvn -Djava.version=21 org.openrewrite.maven:rewrite-maven-plugin:run \
  -Drewrite.configLocation=$(pwd)/kroxylicious-proxy-core/kroxylicious-migrations/src/main/resources/META-INF/rewrite/v0_24.yml \
  -Drewrite.activeRecipes=io.kroxylicious.migrations.v0_24.MigrateTo0_24 \
  -Drewrite.exclusions="**/pom.xml,changelog/unreleased/**"

```

---

## Testing Recipes

Every YAML recipe must have a corresponding JUnit 5 test in `src/test/java/`. Implement `RewriteTest` and supply API stubs via `JavaParser...dependsOn(...)` to isolate tests without introducing heavy dependencies onto the test execution classpath.