# Kroxylicious Migrations

The `kroxylicious-migrations` module provides automated refactoring recipes—powered primarily by [OpenRewrite](https://docs.openrewrite.org/)—to help downstream filter developers seamlessly update their projects when Kroxylicious introduces breaking API changes, package relocations, or deprecations.

Recipes are not limited to Java sources and POMs: they also cover **proxy configuration YAML**. See [Migrating proxy configuration](#migrating-proxy-configuration).

---

## Module Architecture

Migration recipes live inside `src/main/resources/META-INF/rewrite/` as declarative YAML specifications. Each minor release requiring a migration receives its own versioned file, alongside an overarching aggregator file. A recipe whose transformation cannot be expressed declaratively is implemented as a `Recipe` subclass under `src/main/java/` and referenced from the versioned file by fully qualified class name.

```text
kroxylicious-migrations/
└── src/
    └── main/
        ├── java/io/kroxylicious/migrations/
        │   ├── cli/         # the runnable jar's command line (e.g., convert-config)
        │   └── rewrite/
        │       ├── v0_24/   # imperative 0.24.0 recipes (e.g., UseErrorsInsteadOfExceptions)
        │       └── v0_25/   # imperative 0.25.0 recipes (e.g., UseClusterDefinitions)
        └── resources/
            └── META-INF/
                └── rewrite/
                    ├── MigrateToLatest.yml  # Aggregator: MigrateTo
                    ├── v0_24.yml         # 0.24.0 recipes (e.g., MigrateTo0_24)
                    ├── v0_25.yml         # 0.25.0 recipes (e.g., MigrateTo0_25)
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

## Migrating proxy configuration

Some releases change the proxy's configuration YAML as well as its Java API. 0.25.0 is the first such release: `io.kroxylicious.migrations.rewrite.v0_25.UseClusterDefinitions` rewrites the `virtualClusters[].targetCluster` form, deprecated in 0.22.0, into a top-level `clusterDefinitions` list plus a `target: {cluster: ...}` reference.

Because a proxy configuration file can be named anything and live anywhere, the recipe considers **every** YAML file it is given and migrates only those documents that structurally look like a proxy configuration — a mapping with a root level `virtualClusters` sequence, and without the `apiVersion`/`kind` keys that would mark it as a Kubernetes manifest. Pass the `filePattern` option to narrow that down.

### Configuration held anywhere

The migrations jar is runnable, and its `convert-config` command converts the files you name. The files are parsed as YAML and handed straight to the recipes, so no build, no project and no `pom.xml` is involved. [jbang](https://www.jbang.dev/) resolves the dependencies:

```bash
# preview the changes
jbang io.kroxylicious:kroxylicious-migrations:0.25.0 convert-config --dry-run /path/to/kroxylicious-config.yaml

# apply them
jbang io.kroxylicious:kroxylicious-migrations:0.25.0 convert-config /path/to/kroxylicious-config.yaml
```

Either form prints a unified diff of what it changed, or would change. More than one file may be given. Without jbang, run the same command with `java -cp <migrations jar and its dependencies> io.kroxylicious.migrations.cli.KroxyliciousMigrations`.

`convert-config` applies every migration, so it upgrades a configuration from any earlier release in one step, and running it again when there is nothing left to do reports `No changes required.`

### Configuration held inside a Maven or Gradle project

The `dryRun`/`run` invocations above already parse every YAML file under the project, so a configuration file committed alongside your sources is migrated along with the Java sources, as part of `MigrateTo0_25` or `MigrateToLatest`. Nothing extra is needed — but note that the build plugins skip any file which is **both ignored by a `.gitignore` and untracked**, and say nothing when they do, so an ignored configuration file looks exactly like one the recipe declined to migrate. `git check-ignore -v <file>` tells you whether that is what has happened; `convert-config` has no such filter.

### Limitations

* **Anchors and aliases are left alone.** A virtual cluster whose `targetCluster` involves either is skipped, because moving it could change what an alias resolves to. Migrate those by hand.
* **Cluster definitions are not coalesced.** Each migrated virtual cluster gets its own `clusterDefinitions` entry, named `<virtualClusterName>-target`, even where several point at the same Kafka cluster.
* **A comment written after the last key of `targetCluster` stays behind.** In the OpenRewrite YAML model such a comment belongs to the element that follows it, which is the virtual cluster's next key rather than the block being moved.

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