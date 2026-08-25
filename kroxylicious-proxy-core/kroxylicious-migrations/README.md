This module provides migration tools to help project consumers adapt to changes we need to make.   

THe primary mechanism is open-rewrite recipes however other tooling might be used on a case by case basis or as a fallback. 

There will be a published recipe for each version of the project which requires a migration and an aggregator to make all available. 

```
kroxylicious-rewrite/
└── src/main/resources/META-INF/rewrite/
├── v0_24.yml          # Recipes introduced in 0.24.0
├── v1_0.yml           # Recipes introduced in 1.0.0
└── kroxylicious.yml   # Aggregator / Latest recipe
```

to run a migration on the kroxylicious code base:
```shell
mvn -Djava.version=21  org.openrewrite.maven:rewrite-maven-plugin:dryRun \
  -Drewrite.configLocation=$(pwd)/kroxylicious-proxy-core/kroxylicious-migrations/src/main/resources/META-INF/rewrite/v0_24.yml \
  -Drewrite.activeRecipes=io.kroxylicious.migrations.v0_24.MigrateTo0_24 \
-Drewrite.exclusions="**/pom.xml,changelog/unreleased/**"
```
