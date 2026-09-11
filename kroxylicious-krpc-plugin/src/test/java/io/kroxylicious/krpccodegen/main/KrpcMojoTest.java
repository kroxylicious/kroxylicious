/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.main;

import java.nio.file.Path;

import org.apache.maven.api.plugin.testing.InjectMojo;
import org.apache.maven.api.plugin.testing.MojoTest;
import org.apache.maven.plugin.MojoExecutionException;
import org.junit.jupiter.api.Test;

import io.kroxylicious.krpccodegen.maven.KrpcSingleGeneratorMojo;

import static io.kroxylicious.krpccodegen.main.Files.assertFileHasExpectedContents;
import static org.assertj.core.api.Assertions.assertThat;

@MojoTest
class KrpcMojoTest {

    @Test
    void generateSingle(@InjectMojo(goal = "generate-single", pom = "classpath:/test-pom.xml") KrpcSingleGeneratorMojo mojo) throws MojoExecutionException {
        assertThat(mojo).isNotNull();
        mojo.execute();
        Path outdir = Path.of(mojo.project().getBuild().getDirectory()).resolve("outmessage");
        assertThat(outdir).exists().isDirectory();
        Path packageDir = outdir.resolve("com").resolve("example");
        assertThat(packageDir).exists().isDirectory();
        Path generatedFile = packageDir.resolve("FetchRequest.java");
        assertThat(generatedFile).exists().isRegularFile();
        assertFileHasExpectedContents(generatedFile.toFile(), "hello-world/example-expected-FetchRequest.txt");
    }

}
