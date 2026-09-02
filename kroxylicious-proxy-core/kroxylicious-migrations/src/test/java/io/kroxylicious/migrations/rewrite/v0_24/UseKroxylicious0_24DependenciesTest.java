/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.migrations.rewrite.v0_24;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;
import org.openrewrite.HttpSenderExecutionContextView;
import org.openrewrite.InMemoryExecutionContext;
import org.openrewrite.ipc.http.HttpSender;
import org.openrewrite.ipc.http.HttpUrlConnectionSender;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.maven.Assertions.pomXml;

/**
 * Openrewrite validates all versions are valid and available on via HTTP remote.
 * So this test intercepts the HTTP calls and returns stubbed out metadata.
 * We may want to change the approach in future when upgrading other versions,
 * for now however this seems the simplest option as we need to test 0.24.0 prior it's release.
 */
@SuppressWarnings("java:S2699") // rewriteRun contains assertions
class UseKroxylicious0_24DependenciesTest implements RewriteTest {

    private static final Pattern POM_PATTERN = Pattern.compile(".*/(?<artifactId>[^/]+)/(?<version>0\\.\\d+\\.\\d+)/\\k<artifactId>-\\k<version>\\.pom");
    private static final Pattern METADATA_PATTERN = Pattern.compile(".*/(?<artifactId>[^/]+)/maven-metadata.xml");

    private static final String V0_24_0_METADATA = """
            <?xml version="1.0" encoding="UTF-8"?>
            <metadata>
              <groupId>io.kroxylicious</groupId>
              <artifactId>%s</artifactId>
              <versioning>
                <latest>0.24.0</latest>
                <release>0.24.0</release>
                <versions>
                  <version>0.23.0</version>
                  <version>0.24.0</version>
                </versions>
              </versioning>
            </metadata>
            """;

    private static final String V0_24_1_METADATA = """
            <?xml version="1.0" encoding="UTF-8"?>
            <metadata>
              <groupId>io.kroxylicious</groupId>
              <artifactId>%s</artifactId>
              <versioning>
                <latest>0.24.1</latest>
                <release>0.24.1</release>
                <versions>
                  <version>0.23.0</version>
                  <version>0.24.0</version>
                  <version>0.24.1</version>
                </versions>
              </versioning>
            </metadata>
            """;

    private static final String V0_25_METADATA = """
            <?xml version="1.0" encoding="UTF-8"?>
            <metadata>
              <groupId>io.kroxylicious</groupId>
              <artifactId>%s</artifactId>
              <versioning>
                <latest>0.25.0</latest>
                <release>0.25.0</release>
                <versions>
                  <version>0.23.0</version>
                  <version>0.25.0</version>
                </versions>
              </versioning>
            </metadata>
            """;

    private String metadataXml;

    @Override
    public void defaults(RecipeSpec spec) {
        HttpSender stubSender = request -> {

            String requestPath = request.getUrl().getPath();
            Matcher versionedPomMatcher = POM_PATTERN.matcher(requestPath);
            Matcher mmetadataMatcher = METADATA_PATTERN.matcher(requestPath);
            if (mmetadataMatcher.matches()) {
                String artifactId = mmetadataMatcher.group("artifactId");
                return new HttpSender.Response(200, new ByteArrayInputStream(metadataXml.formatted(artifactId).getBytes(StandardCharsets.UTF_8)), () -> {
                });
            }
            else if (versionedPomMatcher.matches()) {
                byte[] buf;
                if (versionedPomMatcher.group("artifactId").equals("kroxylicious-bom")) {
                    String expectedVersion = versionedPomMatcher.group("version");
                    buf = """
                            <project xmlns="http://maven.apache.org/POM/4.0.0"
                                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                     xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
                                <modelVersion>4.0.0</modelVersion>

                                <groupId>io.kroxylicious</groupId>
                                <artifactId>kroxylicious-bom</artifactId>
                                <version>%s</version>
                                <packaging>pom</packaging>

                                <dependencyManagement>
                                    <dependencies>
                                        <dependency>
                                            <groupId>io.kroxylicious</groupId>
                                            <artifactId>kroxylicious-api</artifactId>
                                            <version>%s</version>
                                        </dependency>
                                    </dependencies>
                                </dependencyManagement>
                            </project>
                            """.formatted(expectedVersion, expectedVersion)
                            .getBytes(StandardCharsets.UTF_8);
                }
                else {
                    buf = buildJarPom(versionedPomMatcher);
                }
                return new HttpSender.Response(200, new ByteArrayInputStream(buf), () -> {
                });
            }
            // Delegate real calls to default HTTP sender
            return new HttpUrlConnectionSender().send(request);
        };

        spec.recipeFromResources("io.kroxylicious.migrations.rewrite.v0_24.UseKroxyliciousi0_24")
                .executionContext(
                        HttpSenderExecutionContextView.view(new InMemoryExecutionContext())
                                .setHttpSender(stubSender));

    }

    @Test
    void shouldBumpInlineVersion() {
        metadataXml = V0_24_0_METADATA;
        rewriteRun(
                pomXml(
                        // Before (Input code)
                        """
                                <?xml version="1.0" encoding="UTF-8"?>
                                <project xmlns="http://maven.apache.org/POM/4.0.0"
                                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
                                    <modelVersion>4.0.0</modelVersion>

                                    <groupId>com.example</groupId>
                                    <artifactId>rewrite-victim</artifactId>
                                    <packaging>jar</packaging>
                                    <version>0.0.1-SNAPSHOT</version>

                                    <dependencies>
                                        <dependency>
                                            <groupId>io.kroxylicious</groupId>
                                            <artifactId>kroxylicious-api</artifactId>
                                            <version>0.23.0</version>
                                        </dependency>
                                    </dependencies>
                                </project>
                                """,
                        // After (Expected transformed code)
                        """
                                <?xml version="1.0" encoding="UTF-8"?>
                                <project xmlns="http://maven.apache.org/POM/4.0.0"
                                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
                                    <modelVersion>4.0.0</modelVersion>

                                    <groupId>com.example</groupId>
                                    <artifactId>rewrite-victim</artifactId>
                                    <packaging>jar</packaging>
                                    <version>0.0.1-SNAPSHOT</version>

                                    <dependencies>
                                        <dependency>
                                            <groupId>io.kroxylicious</groupId>
                                            <artifactId>kroxylicious-api</artifactId>
                                            <version>0.24.0</version>
                                        </dependency>
                                    </dependencies>
                                </project>
                                """));
    }

    @Test
    void shouldBumpKms() {
        metadataXml = V0_24_0_METADATA;
        rewriteRun(
                pomXml(
                        // Before (Input code)
                        """
                                <?xml version="1.0" encoding="UTF-8"?>
                                <project xmlns="http://maven.apache.org/POM/4.0.0"
                                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
                                    <modelVersion>4.0.0</modelVersion>

                                    <groupId>com.example</groupId>
                                    <artifactId>rewrite-victim</artifactId>
                                    <packaging>jar</packaging>
                                    <version>0.0.1-SNAPSHOT</version>

                                    <dependencies>
                                        <dependency>
                                            <groupId>io.kroxylicious</groupId>
                                            <artifactId>kroxylicious-kms</artifactId>
                                            <version>0.23.0</version>
                                        </dependency>
                                    </dependencies>
                                </project>
                                """,
                        // After (Expected transformed code)
                        """
                                <?xml version="1.0" encoding="UTF-8"?>
                                <project xmlns="http://maven.apache.org/POM/4.0.0"
                                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
                                    <modelVersion>4.0.0</modelVersion>

                                    <groupId>com.example</groupId>
                                    <artifactId>rewrite-victim</artifactId>
                                    <packaging>jar</packaging>
                                    <version>0.0.1-SNAPSHOT</version>

                                    <dependencies>
                                        <dependency>
                                            <groupId>io.kroxylicious</groupId>
                                            <artifactId>kroxylicious-kms</artifactId>
                                            <version>0.24.0</version>
                                        </dependency>
                                    </dependencies>
                                </project>
                                """));
    }

    @Test
    void shouldBumpPropertyVersion() {
        metadataXml = V0_24_0_METADATA;
        rewriteRun(
                pomXml(
                        // Before (Input code)
                        """
                                <?xml version="1.0" encoding="UTF-8"?>
                                <project xmlns="http://maven.apache.org/POM/4.0.0"
                                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
                                    <modelVersion>4.0.0</modelVersion>

                                    <groupId>com.example</groupId>
                                    <artifactId>rewrite-victim</artifactId>
                                    <packaging>jar</packaging>
                                    <version>0.0.1-SNAPSHOT</version>

                                    <properties>
                                        <kroxylicious.version>0.23.0</kroxylicious.version>
                                    </properties>

                                    <dependencies>
                                        <dependency>
                                            <groupId>io.kroxylicious</groupId>
                                            <artifactId>kroxylicious-api</artifactId>
                                            <version>${kroxylicious.version}</version>
                                        </dependency>
                                    </dependencies>
                                </project>
                                """,
                        // After (Expected transformed code)
                        """
                                <?xml version="1.0" encoding="UTF-8"?>
                                <project xmlns="http://maven.apache.org/POM/4.0.0"
                                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
                                    <modelVersion>4.0.0</modelVersion>

                                    <groupId>com.example</groupId>
                                    <artifactId>rewrite-victim</artifactId>
                                    <packaging>jar</packaging>
                                    <version>0.0.1-SNAPSHOT</version>

                                    <properties>
                                        <kroxylicious.version>0.24.0</kroxylicious.version>
                                    </properties>

                                    <dependencies>
                                        <dependency>
                                            <groupId>io.kroxylicious</groupId>
                                            <artifactId>kroxylicious-api</artifactId>
                                            <version>${kroxylicious.version}</version>
                                        </dependency>
                                    </dependencies>
                                </project>
                                """));
    }

    @Test
    void shouldBumpVersionFromDependencyManagement() {
        metadataXml = V0_24_0_METADATA;
        rewriteRun(
                pomXml(
                        // Before (Input code)
                        """
                                <?xml version="1.0" encoding="UTF-8"?>
                                <project xmlns="http://maven.apache.org/POM/4.0.0"
                                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
                                    <modelVersion>4.0.0</modelVersion>

                                    <groupId>com.example</groupId>
                                    <artifactId>rewrite-victim</artifactId>
                                    <packaging>jar</packaging>
                                    <version>0.0.1-SNAPSHOT</version>

                                    <dependencyManagement>
                                        <dependencies>
                                            <dependency>
                                                <groupId>io.kroxylicious</groupId>
                                                <artifactId>kroxylicious-bom</artifactId>
                                                <version>0.23.0</version>
                                                <scope>import</scope>
                                            </dependency>
                                        </dependencies>
                                    </dependencyManagement>

                                    <dependencies>
                                        <dependency>
                                            <groupId>io.kroxylicious</groupId>
                                            <artifactId>kroxylicious-api</artifactId>
                                        </dependency>
                                    </dependencies>
                                </project>
                                """,
                        // After (Expected transformed code)
                        """
                                <?xml version="1.0" encoding="UTF-8"?>
                                <project xmlns="http://maven.apache.org/POM/4.0.0"
                                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
                                    <modelVersion>4.0.0</modelVersion>

                                    <groupId>com.example</groupId>
                                    <artifactId>rewrite-victim</artifactId>
                                    <packaging>jar</packaging>
                                    <version>0.0.1-SNAPSHOT</version>

                                    <dependencyManagement>
                                        <dependencies>
                                            <dependency>
                                                <groupId>io.kroxylicious</groupId>
                                                <artifactId>kroxylicious-bom</artifactId>
                                                <version>0.24.0</version>
                                                <scope>import</scope>
                                            </dependency>
                                        </dependencies>
                                    </dependencyManagement>

                                    <dependencies>
                                        <dependency>
                                            <groupId>io.kroxylicious</groupId>
                                            <artifactId>kroxylicious-api</artifactId>
                                        </dependency>
                                    </dependencies>
                                </project>
                                """));
    }

    @Test
    void shouldBumpToLatestPatchVersion() {
        metadataXml = V0_24_1_METADATA;
        rewriteRun(
                pomXml(
                        // Before (Input code)
                        """
                                <?xml version="1.0" encoding="UTF-8"?>
                                <project xmlns="http://maven.apache.org/POM/4.0.0"
                                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
                                    <modelVersion>4.0.0</modelVersion>

                                    <groupId>com.example</groupId>
                                    <artifactId>rewrite-victim</artifactId>
                                    <packaging>jar</packaging>
                                    <version>0.0.1-SNAPSHOT</version>

                                    <dependencies>
                                        <dependency>
                                            <groupId>io.kroxylicious</groupId>
                                            <artifactId>kroxylicious-api</artifactId>
                                            <version>0.23.0</version>
                                        </dependency>
                                    </dependencies>
                                </project>
                                """,
                        // After (Expected transformed code)
                        """
                                <?xml version="1.0" encoding="UTF-8"?>
                                <project xmlns="http://maven.apache.org/POM/4.0.0"
                                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
                                    <modelVersion>4.0.0</modelVersion>

                                    <groupId>com.example</groupId>
                                    <artifactId>rewrite-victim</artifactId>
                                    <packaging>jar</packaging>
                                    <version>0.0.1-SNAPSHOT</version>

                                    <dependencies>
                                        <dependency>
                                            <groupId>io.kroxylicious</groupId>
                                            <artifactId>kroxylicious-api</artifactId>
                                            <version>0.24.1</version>
                                        </dependency>
                                    </dependencies>
                                </project>
                                """));
    }

    @Test
    void shouldNotBumpIfAlreadyNewer() {
        metadataXml = V0_25_METADATA;
        rewriteRun(
                pomXml(
                        // Before (Input code)
                        """
                                <?xml version="1.0" encoding="UTF-8"?>
                                <project xmlns="http://maven.apache.org/POM/4.0.0"
                                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
                                    <modelVersion>4.0.0</modelVersion>

                                    <groupId>com.example</groupId>
                                    <artifactId>rewrite-victim</artifactId>
                                    <packaging>jar</packaging>
                                    <version>0.0.1-SNAPSHOT</version>

                                    <dependencies>
                                        <dependency>
                                            <groupId>io.kroxylicious</groupId>
                                            <artifactId>kroxylicious-api</artifactId>
                                            <version>0.25.0</version>
                                        </dependency>
                                    </dependencies>
                                </project>
                                """));
    }

    private static byte[] buildJarPom(Matcher versionedPomMatcher) {
        String expectedVersion = versionedPomMatcher.group("version");
        String expectedArtifact = versionedPomMatcher.group("artifactId");
        return """
                <project xmlns="http://maven.apache.org/POM/4.0.0"
                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
                    <modelVersion>4.0.0</modelVersion>

                    <parent>
                        <groupId>io.kroxylicious</groupId>
                        <artifactId>kroxylicious-parent</artifactId>
                        <version>%s</version>
                        <relativePath>../pom.xml</relativePath>
                    </parent>

                    <artifactId>%s</artifactId>

                </project>
                """.formatted(expectedVersion, expectedArtifact)
                .getBytes(StandardCharsets.UTF_8);
    }

}