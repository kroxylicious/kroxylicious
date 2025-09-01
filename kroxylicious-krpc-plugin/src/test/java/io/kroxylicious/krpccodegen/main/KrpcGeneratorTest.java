/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.common.io.Files;
import com.google.common.io.Resources;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.writeString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
    void testHelloWorld(@TempDir File tempDir) throws Exception {
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
    void singleGenerateDoesNotModifyFileIfContentsUnchanged(@TempDir File tempDir) throws Exception {
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
        long lastModified = file.lastModified();
        Thread.sleep(10);
        gen.generate();
        long lastModifiedAfterGen = file.lastModified();
        assertThat(lastModifiedAfterGen).isEqualTo(lastModified);
    }

    @Test
    void testKrpcData(@TempDir File tempDir) throws Exception {
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
    void testKproxyFilter(@TempDir File tempDir) throws Exception {
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
    void testKproxyRequestFilter(@TempDir File tempDir) throws Exception {
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

    @Test
    void multiGenerateDoesNotModifyFileIfContentsUnchanged(@TempDir File tempDir) throws Exception {
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
        long lastModified = file.lastModified();
        Thread.sleep(10);
        gen.generate();
        long lastModifiedAfterGen = file.lastModified();
        assertThat(lastModifiedAfterGen).isEqualTo(lastModified);
    }

    @ParameterizedTest
    @CsvSource({ "AddPartitionsToTxnRequest.json,yes,no", "FetchRequest.json,no,no" })
    void testLatestVersionUnstable(String messageSpec, String expectField, String expectValue, @TempDir File tempDir) throws Exception {
        testSingleGeneration(tempDir, messageSpec, "${messageSpec.latestVersionUnstable.isPresent()?string('yes', 'no')}", expectField);
        testSingleGeneration(tempDir, messageSpec, "${messageSpec.latestVersionUnstable.orElse(false)?string('yes', 'no')}", expectValue);
    }

    @CsvSource(nullValues = "null", value = { "a,b,false", "a,a,true", "aa,a,false", "a,aa,false", "a,null,false" })
    @ParameterizedTest
    void filesEqual(String fileAContent, String fileBContent, boolean expectedEquality, @TempDir File tempDir) throws IOException {
        Path fileA = tempDir.toPath().resolve("a");
        Path fileB = tempDir.toPath().resolve("b");
        writeString(fileA, fileAContent, UTF_8);
        if (fileBContent != null) {
            writeString(fileB, fileBContent, UTF_8);
        }
        boolean filesEqual = KrpcGenerator.filesEqual(fileA, fileB);
        assertThat(filesEqual).isEqualTo(expectedEquality);
    }

    @Test
    void largeFilesEqual(@TempDir File tempDir) throws IOException {
        Path fileA = tempDir.toPath().resolve("a");
        Path fileB = tempDir.toPath().resolve("b");
        try (BufferedWriter bufferedWriter = java.nio.file.Files.newBufferedWriter(fileA);
                BufferedWriter bufferedWriterB = java.nio.file.Files.newBufferedWriter(fileB)) {
            for (int i = 0; i < 100000; i++) {
                bufferedWriter.write("Hello, world!");
                bufferedWriterB.write("Hello, world!");
                bufferedWriter.newLine();
                bufferedWriterB.newLine();
            }
        }
        boolean filesEqual = KrpcGenerator.filesEqual(fileA, fileB);
        assertThat(filesEqual).isTrue();
    }

    public static Stream<Arguments> filesEqualInvalidArguments() {
        return Stream.of(Arguments.argumentSet("generated file does not exist", (Consumer<Path>) tempDir -> {
            Path fileB = tempDir.resolve("b");
            touch(fileB);
            Path fileA = tempDir.resolve("a");
            KrpcGenerator.filesEqual(fileA, fileB);
        }, IllegalArgumentException.class, "does not exist"),
                Arguments.argumentSet("generated file is not a file", (Consumer<Path>) tempDir -> {
                    Path fileB = tempDir.resolve("b");
                    touch(fileB);
                    KrpcGenerator.filesEqual(tempDir, fileB);
                }, IllegalArgumentException.class, "exists but is not a regular file"),
                Arguments.argumentSet("generated file null", (Consumer<Path>) tempDir -> {
                    Path fileB = tempDir.resolve("b");
                    touch(fileB);
                    KrpcGenerator.filesEqual(null, fileB);
                }, NullPointerException.class, "generatedFile cannot be null"),
                Arguments.argumentSet("final file null", (Consumer<Path>) tempDir -> {
                    Path fileB = tempDir.resolve("b");
                    touch(fileB);
                    KrpcGenerator.filesEqual(fileB, null);
                }, NullPointerException.class, "finalFile cannot be null"),
                Arguments.argumentSet("final file is not a file", (Consumer<Path>) tempDir -> {
                    Path fileA = tempDir.resolve("b");
                    touch(fileA);
                    KrpcGenerator.filesEqual(fileA, tempDir);
                }, IllegalArgumentException.class, "exists but is not a regular file"));
    }

    private static void touch(Path fileB) {
        try {
            java.nio.file.Files.createFile(fileB);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @MethodSource
    @ParameterizedTest
    void filesEqualInvalidArguments(Consumer<Path> tempDirConsumer, Class<? extends Exception> exceptionType, String message, @TempDir File tempDir) {
        Path path = tempDir.toPath();
        assertThatThrownBy(() -> tempDirConsumer.accept(path)).isInstanceOf(exceptionType).hasMessageContaining(message);
    }

    private void assertFileHasExpectedContents(File file, String expectedFile) throws IOException {
        String expected = Resources.asCharSource(
                Objects.requireNonNull(getClass().getClassLoader().getResource(expectedFile)), UTF_8).read();
        assertThat(file).content().isEqualTo(expected);
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
