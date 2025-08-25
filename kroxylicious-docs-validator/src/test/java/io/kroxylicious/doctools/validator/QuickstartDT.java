/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doctools.validator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.asciidoctor.Attributes;
import org.asciidoctor.ast.StructuralNode;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.doctools.asciidoc.Block;
import io.kroxylicious.doctools.asciidoc.BlockExtractor;
import io.kroxylicious.test.ShellUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Executes the shell commands found within quickstart, in sequence, which a single shell.
 * Test will fail if any command fails.
 */
@EnabledIf("io.kroxylicious.doctools.validator.QuickstartDT#isEnvironmentValid")
@SuppressWarnings("java:S3577") // ignoring naming convention for the test class
class QuickstartDT {

    private static final FileAttribute<Set<PosixFilePermission>> OWNER_RWX = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));

    private static Stream<Arguments> quickstarts() {
        try (var blockExtractor = new BlockExtractor()) {

            Assertions.assertThat(Utils.OPERATOR_ZIP).exists();

            // Some quickstarts rely on the {OperatorAssetZipLink} variable
            blockExtractor.withAttributes(Attributes.builder()
                    .attribute("OperatorAssetZipLink", pathToFileUrl(Utils.OPERATOR_ZIP))
                    .build());

            var recordEncryptionQuickstart = Utils.DOCS_ROOTDIR.resolve("record-encryption-quickstart").resolve("index.adoc");
            var quickstarts = List.of(new Quickstart("record-encryption-quickstart(vault)", recordEncryptionQuickstart, blockIsInvariantOrMatches("kms", "vault")),
                    new Quickstart("record-encryption-quickstart(localstack)", recordEncryptionQuickstart, blockIsInvariantOrMatches("kms", "localstack")));
            return quickstarts.stream().map(q -> extractCodeBlocks(blockExtractor, q));
        }
    }

    @ParameterizedTest
    @MethodSource("quickstarts")
    void quickstart(List<Block> shellBlocks) {
        Path shellScript = writeShellScript(shellBlocks);

        executeScript(shellScript);
    }

    private static String pathToFileUrl(Path path) {
        try {
            return path.toFile().toURI().toURL().toString();
        }
        catch (MalformedURLException e) {
            throw new UncheckedIOException("Failed to express %s as a URL".formatted(path), e);
        }
    }

    private static Predicate<StructuralNode> blockIsInvariantOrMatches(String variantKey, String variantValue) {
        return sn -> {
            var value = sn.getAttribute(variantKey);
            return value == null || Objects.equals(variantValue, value);
        };
    }

    private static Arguments extractCodeBlocks(BlockExtractor extractor, Quickstart qs) {
        var pred = isShellBlock().and(qs.selector());
        var cmds = extractor.extract(qs.path(), pred);
        return Arguments.argumentSet(qs.name(), cmds);
    }

    private static Predicate<StructuralNode> isShellBlock() {
        return sn -> Objects.equals(sn.getAttribute("style", null), "source") &&
                Objects.equals(sn.getAttribute("language", null), "terminal");
    }

    private void executeScript(Path shellScript) {
        var builder = new ProcessBuilder(shellScript.toAbsolutePath().toString());

        var stdoutExecutor = Executors.newSingleThreadExecutor();
        var stderrExecutor = Executors.newSingleThreadExecutor();
        try {
            var p = builder.start();
            try {
                p.getOutputStream().close();

                var readOutput = CompletableFuture.supplyAsync(() -> streamToString(p.getInputStream()), stdoutExecutor);
                var readStderr = CompletableFuture.supplyAsync(() -> streamToString(p.getErrorStream()), stderrExecutor);

                p.waitFor(5, TimeUnit.MINUTES);
                var stdout = readOutput.join();
                var stderr = readStderr.join();

                assertThat(p.exitValue())
                        .describedAs("Quickstart failed - examine stdout/stderr for details %s/%s", stdout, stderr)
                        .isZero();
            }
            finally {
                p.destroy();
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to run script containing quickstart commands: %s".formatted(shellScript), e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to run script containing quickstart commands: %s".formatted(shellScript), e);
        }
        finally {
            stdoutExecutor.shutdown();
            stderrExecutor.shutdown();
        }
    }

    private static String streamToString(InputStream inputStream) {
        try (var reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
                builder.append(String.format("%n"));
            }

            return builder.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path writeShellScript(List<Block> shellBlocks) {
        try {
            var tempFile = Files.createTempFile("quickstart", ".sh", OWNER_RWX);
            try (var writer = new PrintWriter(Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8))) {
                writer.println("#!/usr/bin/env sh");
                writer.println("set -e -v -o pipefail");
                shellBlocks.forEach(block -> {
                    try (var reader = new BufferedReader(new StringReader(block.content()))) {
                        writer.println("""
                                echo "##############"
                                echo "Code block source: %s (line %s)"
                                """.formatted(block.asciiDocFile(), block.lineNumber()));
                        String line;
                        while ((line = reader.readLine()) != null) {
                            line = line.replaceAll("^\\$ *", ""); // chomp the shell prompt
                            writer.println(line);
                        }
                        writer.println();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException("Failed to write block %s to temporary shell file %s".formatted(block, tempFile), e);
                    }
                });

                return tempFile;
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    record Quickstart(String name, Path path, Predicate<StructuralNode> selector) {}

    public static boolean isEnvironmentValid() {
        return ShellUtils.validateToolsOnPath("minikube") && ShellUtils.validateKubeContext("minikube");
    }

}
