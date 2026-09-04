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
                    ├── MigrateToLatest.yml  # Aggregator: MigrateTo
                    ├── v0_24.yml         # 0.24.0 recipes (e.g., MigrateTo0_24)
                    └── v1_0.yml          # 1.0.0 recipes

```

### Recipe Design Guidelines

* **Atomic Recipes (`Use...`):** Focus on a single structural change (e.g., `io.kroxylicious.migrations.rewrite.v0_24.UseKroxyliciousKafkaTypes`).
* **Version Aggregators (`MigrateTo...`):** Combines all atomic recipes for a specific release (e.g., `io.kroxylicious.migrations.rewrite.v0_24.MigrateTo0_24`).
* **Latest Aggregator (`MigrateToLatest`):** Combines all historical version aggregators in `MigrateToLatest.yml` so downstream projects can jump across multiple minor versions in one step.

---

## Downstream Execution (Filter Developers)

Downstream filter projects do not need to modify their `pom.xml` to execute published migrations. The migrations will attempt to bump Java dependencies to match the required Kroxylicious release. These version bumps compare versions semantically and only ever move forward, so it's safe to run a migration even if you're starting from an intermediate release — it won't downgrade a dependency you've already upgraded. They can run OpenRewrite directly from the command line against published Maven Central artifacts:

**Apply a Specific Release Migration:**

[OpenRewrite](https://github.com/openrewrite/rewrite) allows you to see the changes that our migrations would make to your code via the `dryRun` goal. The examples below assume maven for other build tools see the [OpenRewrite docs](https://docs.openrewrite.org/running-recipes/getting-started#step-6-running-recipes-from-external-modules). 

### dry run
```bash
mvn org.openrewrite.maven:rewrite-maven-plugin:dryRun \
  -Drewrite.recipeArtifactCoordinates=io.kroxylicious:kroxylicious-migrations:0.25.0-SNAPSHOT \
  -Drewrite.activeRecipes=io.kroxylicious.migrations.rewrite.v0_24.MigrateTo0_24
```

Review the proposed patch file, if your happy apply it using `run`:

### Run
```bash
mvn org.openrewrite.maven:rewrite-maven-plugin:run \
  -Drewrite.recipeArtifactCoordinates=io.kroxylicious:kroxylicious-migrations:0.25.0-SNAPSHOT \
  -Drewrite.activeRecipes=io.kroxylicious.migrations.rewrite.v0_24.MigrateTo0_24
```

**Upgrade Across Multiple Releases to Latest:**

```bash
mvn org.openrewrite.maven:rewrite-maven-plugin:run \
  -Drewrite.recipeArtifactCoordinates=io.kroxylicious:kroxylicious-migrations:0.25.0-SNAPSHOT \
  -Drewrite.activeRecipes=io.kroxylicious.migrations.rewrite.MigrateToLatest
```

**Gradle**

Add the plugin and declare `kroxylicious-migrations` as a `rewrite` dependency so its recipes are on the classpath:

```kotlin
plugins {
    id("org.openrewrite.rewrite") version "6.x.x"
}

dependencies {
    rewrite("io.kroxylicious:kroxylicious-migrations:0.25.0-SNAPSHOT
}
```

Then select the recipe to run on the command line, just as with the Maven examples above:

```bash
# Preview changes
./gradlew rewriteDryRun -Drewrite.activeRecipe=io.kroxylicious.migrations.rewrite.v0_24.MigrateTo0_24

# Apply changes
./gradlew rewriteRun -Drewrite.activeRecipe=io.kroxylicious.migrations.rewrite.v0_24.MigrateTo0_24

# Upgrade across multiple releases to latest
./gradlew rewriteRun -Drewrite.activeRecipe=io.kroxylicious.migrations.rewrite.MigrateToLatest
```

---

## In-Tree Execution (Core Contributors)

When developing or validating recipes against the local Kroxylicious repository, pass the absolute path using `$(pwd)` to bypass local artifact installation (`mvn install`):

> NOTE: the `java.version` is required in order to avoid a maven property conflict between Rewrite and properties used by Kroxylicious's own POMs.

**Preview Changes (Dry Run):**

```bash
mvn -Djava.version=21 org.openrewrite.maven:rewrite-maven-plugin:dryRun \
  -Drewrite.configLocation=$(pwd)/kroxylicious-proxy-core/kroxylicious-migrations/src/main/resources/META-INF/rewrite/v0_24.yml \
  -Drewrite.activeRecipes=io.kroxylicious.migrations.rewrite.v0_24.MigrateTo0_24 \
  -Drewrite.exclusions="**/pom.xml,changelog/unreleased/**,kroxylicious-proxy-core/kroxylicious-migrations/**,tools/**,kroxylicious-wire-fidelity-tests/**"
```

> NOTE: the `kroxylicious-migrations` module is excluded because the recipes would otherwise rewrite the package names embedded in the recipes' own source and in the `String` literals of their tests.

**Apply Changes In-Place:**

```bash
mvn -Djava.version=21 org.openrewrite.maven:rewrite-maven-plugin:run \
  -Drewrite.configLocation=$(pwd)/kroxylicious-proxy-core/kroxylicious-migrations/src/main/resources/META-INF/rewrite/v0_24.yml \
  -Drewrite.activeRecipes=io.kroxylicious.migrations.rewrite.v0_24.MigrateTo0_24 \
  -Drewrite.exclusions="**/pom.xml,changelog/unreleased/**,kroxylicious-proxy-core/kroxylicious-migrations/**,tools/**,kroxylicious-wire-fidelity-tests/**"

```

---

## Testing Recipes

Every YAML recipe must have a corresponding JUnit 5 test in `src/test/java/`. Implement `RewriteTest` and supply API stubs via `JavaParser...dependsOn(...)` to isolate tests without introducing heavy dependencies onto the test execution classpath.