/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

public class webifyTest {

    public static final String PROJECT_VERSION = "1.2.3";

    @Test
    void webify(@TempDir Path sourceDir, @TempDir Path destDir) throws Exception {
        Path outputHtmlPath = destDir.resolve("documentation").resolve(PROJECT_VERSION).resolve("html");
        Path outputDataPath = destDir.resolve("_data").resolve("documentation");
        Files.createDirectories(outputHtmlPath);
        Files.createDirectories(outputDataPath);
        Files.createDirectories(sourceDir.resolve("developer-guide"));
        Files.createDirectories(sourceDir.resolve("other-guide"));
        copyFileFromClasspath(sourceDir, "developer-guide/doc.yaml");
        copyFileFromClasspath(sourceDir, "developer-guide/index.html");
        copyFileFromClasspath(sourceDir, "other-guide/doc.yaml");
        copyFileFromClasspath(sourceDir, "other-guide/index.html");
        webify.execute("--project-version=" + PROJECT_VERSION,
                "--src-dir=" + sourceDir.toAbsolutePath(),
                "--dest-dir=" + destDir.toAbsolutePath(),
                "--tocify=*/index.html",
                "--tocify-toc-file=toc.html",
                "--tocify-tocless-file=content.html",
                "--datafy=*/doc.yaml");
        assertContentEquals(outputHtmlPath.resolve("developer-guide").resolve("content.html"), """
                {% raw %}
                <!--

                    Copyright Kroxylicious Authors.

                    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

                -->
                <html>
                 <head></head>
                 <body>
                  <div id="preamble"></div>
                 </body>
                </html>
                {% endraw %}
                """);
        assertContentEquals(outputHtmlPath.resolve("other-guide").resolve("content.html"), """
                {% raw %}
                <!--

                    Copyright Kroxylicious Authors.

                    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

                -->
                <html>
                 <head></head>
                 <body>
                  <div id="preamble"></div>
                 </body>
                </html>
                {% endraw %}
                """);
        assertContentEquals(outputHtmlPath.resolve("other-guide").resolve("toc.html"), """
                {% raw %}
                <div id="toc" class="toc">
                 <ul class="sectlevel1">
                  <li><a href="#assembly-proxy-overview-proxy">1. Kroxylicious Other overview</a>
                   <ul class="sectlevel2">
                    <li><a href="#con-api-compatibility-developer-proxy">1.1. Compatibility</a></li>
                   </ul></li>
                  <li><a href="#con-custom-filters-proxy">2. Custom filters</a>
                   <ul class="sectlevel2">
                    <li><a href="#custom_filter_project_generation">2.1. Custom Filter Project Generation</a></li>
                    <li><a href="#api_docs">2.2. API docs</a></li>
                    <li><a href="#dependencies">2.3. Dependencies</a></li>
                    <li><a href="#protocol_filters">2.4. Protocol filters</a></li>
                    <li><a href="#packaging_filters">2.5. Packaging filters</a></li>
                   </ul></li>
                  <li><a href="#trademark_notice">3. Trademark notice</a></li>
                 </ul>
                </div>
                {% endraw %}
                """);
        assertContentEquals(outputHtmlPath.resolve("developer-guide").resolve("toc.html"), """
                {% raw %}
                <div id="toc" class="toc">
                 <ul class="sectlevel1">
                  <li><a href="#assembly-proxy-overview-proxy">1. Kroxylicious Proxy overview</a>
                   <ul class="sectlevel2">
                    <li><a href="#con-api-compatibility-developer-proxy">1.1. Compatibility</a></li>
                   </ul></li>
                  <li><a href="#con-custom-filters-proxy">2. Custom filters</a>
                   <ul class="sectlevel2">
                    <li><a href="#custom_filter_project_generation">2.1. Custom Filter Project Generation</a></li>
                    <li><a href="#api_docs">2.2. API docs</a></li>
                    <li><a href="#dependencies">2.3. Dependencies</a></li>
                    <li><a href="#protocol_filters">2.4. Protocol filters</a></li>
                    <li><a href="#packaging_filters">2.5. Packaging filters</a></li>
                   </ul></li>
                  <li><a href="#trademark_notice">3. Trademark notice</a></li>
                 </ul>
                </div>
                {% endraw %}
                """);
        assertContentEquals(outputDataPath.resolve(PROJECT_VERSION.replace(".", "_") + ".yaml"), """
                docs:
                  - title: Kroxylicious Developer guide
                    description: Covers writing plugins for the proxy in the Java programming language
                    tags:
                      - developer
                    rank: '032'
                    path: html/developer-guide
                  - title: Kroxylicious other guide
                    description: Covers other stuff
                    tags:
                      - developer
                    rank: '032'
                    path: html/other-guide
                """);
    }

    private void assertContentEquals(Path resolve, String expectedContents) {
        assertThat(Files.exists(resolve)).isTrue();
        assertThat(Files.isRegularFile(resolve)).isTrue();
        try {
            assertThat(Files.readString(resolve)).isEqualTo(expectedContents);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void copyFileFromClasspath(Path sourceDir, String fileName) throws IOException {
        Path testFile = sourceDir.resolve(fileName);
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(fileName)) {
            Assertions.assertNotNull(is);
            Files.copy(is, testFile);
        }
    }
}
