/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.main;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.google.common.io.Files;
import com.google.common.io.Resources;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class KrpcGeneratorTest {

    private static final String MESSAGE_SPECS_PATH = "message-specs/common/message";
    private static final String TEST_CLASSES_DIR = "test-classes";

    @Test
    void testHelloWorld(
            @TempDir
            File tempDir
    ) throws Exception {
        KrpcGenerator gen = KrpcGenerator.single()
                                         .withMessageSpecDir(getMessageSpecDir())
                                         .withMessageSpecFilter("*.json")
                                         .withTemplateDir(getTemplateDir())
                                         .withTemplateNames(List.of("hello-world/example.ftl"))
                                         .withOutputPackage("com.foo")
                                         .withOutputDir(tempDir)
                                         .withOutputFilePattern("${messageSpecName}.txt")
                                         .build();

        gen.generate();

        File file = join(tempDir, "com", "foo", "FetchRequest.txt");
        assertFileHasExpectedContents(file, "hello-world/example-expected-FetchRequest.txt");
    }

    @Test
    void testKrpcData(
            @TempDir
            File tempDir
    ) throws Exception {
        KrpcGenerator gen = KrpcGenerator.single()
                                         .withMessageSpecDir(getMessageSpecDir())
                                         .withMessageSpecFilter("*.json")
                                         .withTemplateDir(getTemplateDir())
                                         .withTemplateNames(List.of("Data/example.ftl"))
                                         .withOutputPackage("com.foo")
                                         .withOutputDir(tempDir)
                                         .withOutputFilePattern("${messageSpecName}.java")
                                         .build();

        gen.generate();

        File file = join(tempDir, "com", "foo", "FetchRequest.java");
        assertFileHasExpectedContents(file, "Data/example-expected-FetchRequest.java.txt");
    }

    @Test
    void testKproxyFilter(
            @TempDir
            File tempDir
    ) throws Exception {
        KrpcGenerator gen = KrpcGenerator.single()
                                         .withMessageSpecDir(getMessageSpecDir())
                                         .withMessageSpecFilter("*.json")
                                         .withTemplateDir(getTemplateDir())
                                         .withTemplateNames(List.of("Kproxy/Filter.ftl"))
                                         .withOutputPackage("com.foo")
                                         .withOutputDir(tempDir)
                                         .withOutputFilePattern("${messageSpecName}Filter.java")
                                         .build();

        gen.generate();

        File file = join(tempDir, "com", "foo", "FetchRequestFilter.java");
        assertFileHasExpectedContents(file, "Kproxy/Filter-expected-FetchRequestFilter.java.txt");
    }

    @Test
    void testKproxyRequestFilter(
            @TempDir
            File tempDir
    ) throws Exception {
        KrpcGenerator gen = KrpcGenerator.multi()
                                         .withMessageSpecDir(getMessageSpecDir())
                                         .withMessageSpecFilter("*{Request}.json")
                                         .withTemplateDir(getTemplateDir())
                                         .withTemplateNames(List.of("Kproxy/KrpcRequestFilter.ftl"))
                                         .withOutputPackage("com.foo")
                                         .withOutputDir(tempDir)
                                         .withOutputFilePattern("${templateName}.java")
                                         .build();

        gen.generate();

        File file = join(tempDir, "com", "foo", "KrpcRequestFilter.java");
        assertFileHasExpectedContents(file, "Kproxy/KrpcRequestFilter-expected.txt");
    }

    @ParameterizedTest
    @CsvSource(
        { "AddPartitionsToTxnRequest.json,yes,no", "FetchRequest.json,no,no" }
    )
    void testLatestVersionUnstable(
            String messageSpec,
            String expectField,
            String expectValue,
            @TempDir
            File tempDir
    ) throws Exception {
        testSingleGeneration(tempDir, messageSpec, "${messageSpec.latestVersionUnstable.isPresent()?string('yes', 'no')}", expectField);
        testSingleGeneration(tempDir, messageSpec, "${messageSpec.latestVersionUnstable.orElse(false)?string('yes', 'no')}", expectValue);
    }

    private void assertFileHasExpectedContents(File file, String expectedFile) throws IOException {
        String expected = Resources.asCharSource(
                Objects.requireNonNull(getClass().getClassLoader().getResource(expectedFile)),
                UTF_8
        ).read();
        assertThat(file).content().isEqualToIgnoringWhitespace(expected);
    }

    private static void testSingleGeneration(File tempDir, String messageSpec, String template, String expectedContents) throws Exception {
        String templateFile = "template.ftl";
        String outputFile = "output.txt";
        Files.asCharSink(new File(tempDir, templateFile), UTF_8).write(template);
        KrpcGenerator gen = KrpcGenerator.single()
                                         .withMessageSpecDir(getMessageSpecDir())
                                         .withMessageSpecFilter(messageSpec)
                                         .withTemplateDir(tempDir)
                                         .withTemplateNames(List.of(templateFile))
                                         .withOutputPackage("com.foo")
                                         .withOutputDir(tempDir)
                                         .withOutputFilePattern(outputFile)
                                         .build();
        gen.generate();
        File file = join(tempDir, "com", "foo", outputFile);
        assertThat(file).hasContent(expectedContents);
    }

    private static File join(File dir, String... pathElements) {
        StringJoiner stringJoiner = new StringJoiner(File.separator);
        for (String pathElement : pathElements) {
            stringJoiner.add(pathElement);
        }
        return new File(dir, stringJoiner.toString());
    }

    private static File getMessageSpecDir() {
        return getBuildDir().resolve(MESSAGE_SPECS_PATH).toFile();
    }

    private static File getTemplateDir() {
        return getBuildDir().resolve(TEST_CLASSES_DIR).toFile();
    }

    private static Path getBuildDir() {
        try {
            return Paths.get(KrpcGeneratorTest.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent();
        }
        catch (URISyntaxException e) {
            throw new RuntimeException("Couldn't resolve build directory", e);
        }
    }
}
