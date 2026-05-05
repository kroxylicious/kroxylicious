/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class ApiVersionProcessorTest {

    @TempDir
    Path outputDir;

    @Test
    void shouldGenerateResourceForPluginWithApiVersion() {
        var result = compile(
                source("io.example.MyApi",
                        """
                                package io.example;
                                import io.kroxylicious.proxy.plugin.ApiVersion;
                                @ApiVersion("v1beta1")
                                public interface MyApi {}
                                """),
                source("io.example.MyPlugin",
                        """
                                package io.example;
                                import io.kroxylicious.proxy.plugin.Plugin;
                                @Plugin(configType = Void.class)
                                public class MyPlugin implements MyApi {}
                                """));

        assertThat(result.success()).isTrue();
        Path resource = outputDir.resolve(
                ApiVersionProcessor.RESOURCE_PREFIX + "io.example.MyPlugin");
        assertThat(resource).exists().content().isEqualTo("io.example.MyApi:v1beta1\n");
    }

    @Test
    void shouldGenerateResourceForIndirectApiVersion() {
        var result = compile(
                source("io.example.BaseApi",
                        """
                                package io.example;
                                import io.kroxylicious.proxy.plugin.ApiVersion;
                                @ApiVersion("v2")
                                public interface BaseApi {}
                                """),
                source("io.example.SubApi",
                        """
                                package io.example;
                                public interface SubApi extends BaseApi {}
                                """),
                source("io.example.MyPlugin",
                        """
                                package io.example;
                                import io.kroxylicious.proxy.plugin.Plugin;
                                @Plugin(configType = Void.class)
                                public class MyPlugin implements SubApi {}
                                """));

        assertThat(result.success()).isTrue();
        Path resource = outputDir.resolve(
                ApiVersionProcessor.RESOURCE_PREFIX + "io.example.MyPlugin");
        assertThat(resource).exists().content().isEqualTo("io.example.BaseApi:v2\n");
    }

    @Test
    void shouldRecordMultipleVersionedInterfaces() {
        var result = compile(
                source("io.example.ApiOne",
                        """
                                package io.example;
                                import io.kroxylicious.proxy.plugin.ApiVersion;
                                @ApiVersion("v1")
                                public interface ApiOne {}
                                """),
                source("io.example.ApiTwo",
                        """
                                package io.example;
                                import io.kroxylicious.proxy.plugin.ApiVersion;
                                @ApiVersion("v1alpha1")
                                public interface ApiTwo {}
                                """),
                source("io.example.MyPlugin",
                        """
                                package io.example;
                                import io.kroxylicious.proxy.plugin.Plugin;
                                @Plugin(configType = Void.class)
                                public class MyPlugin implements ApiOne, ApiTwo {}
                                """));

        assertThat(result.success()).isTrue();
        Path resource = outputDir.resolve(
                ApiVersionProcessor.RESOURCE_PREFIX + "io.example.MyPlugin");
        assertThat(resource).exists().content()
                .contains("io.example.ApiOne:v1\n")
                .contains("io.example.ApiTwo:v1alpha1\n");
    }

    @Test
    void shouldWarnWhenNoApiVersionOnInterface() {
        var result = compile(
                source("io.example.UnversionedApi",
                        """
                                package io.example;
                                public interface UnversionedApi {}
                                """),
                source("io.example.MyPlugin",
                        """
                                package io.example;
                                import io.kroxylicious.proxy.plugin.Plugin;
                                @Plugin(configType = Void.class)
                                public class MyPlugin implements UnversionedApi {}
                                """));

        assertThat(result.success()).isTrue();
        assertThat(result.diagnostics().getDiagnostics())
                .anyMatch(d -> d.getKind() == javax.tools.Diagnostic.Kind.WARNING
                        && d.getMessage(Locale.ROOT).contains("@ApiVersion"));

        Path resource = outputDir.resolve(
                ApiVersionProcessor.RESOURCE_PREFIX + "io.example.MyPlugin");
        assertThat(resource).doesNotExist();
    }

    record CompilationResult(boolean success,
                             DiagnosticCollector<JavaFileObject> diagnostics) {}

    private CompilationResult compile(JavaFileObject... sources) {
        var compiler = ToolProvider.getSystemJavaCompiler();
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        var task = compiler.getTask(
                null,
                null,
                diagnostics,
                List.of("-d", outputDir.toString(), "-proc:only", "-processor",
                        ApiVersionProcessor.class.getName()),
                null,
                List.of(sources));
        boolean success = task.call();
        return new CompilationResult(success, diagnostics);
    }

    private static JavaFileObject source(String qualifiedName, String code) {
        return new SimpleJavaFileObject(
                URI.create("string:///" + qualifiedName.replace('.', '/') + ".java"),
                JavaFileObject.Kind.SOURCE) {
            @Override
            public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                return code;
            }
        };
    }
}
