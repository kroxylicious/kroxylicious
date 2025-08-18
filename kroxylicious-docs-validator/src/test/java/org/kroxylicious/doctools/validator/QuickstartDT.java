/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package org.kroxylicious.doctools.validator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.asciidoctor.Attributes;
import org.asciidoctor.ast.StructuralNode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.kroxylicious.doctools.asciidoc.Block;
import org.kroxylicious.doctools.asciidoc.BlockExtractor;

import static org.assertj.core.api.Assertions.assertThat;

/** Executes the quickstart commands in sequence. Test will fail if any command fails. */
class QuickstartTest {
    private static Stream<Arguments> quickstarts() {
        try (var blockExtractor = new BlockExtractor()) {

            // FIXME - should point to zip built by build.
            blockExtractor.withAttributes(Attributes.builder().attribute("OperatorAssetZipLink", "https://github.com/kroxylicious/kroxylicious/releases/download/v0.15.0/kroxylicious-operator-0.15.0.zip").build());

            var recordEncryptionQuickstart = Utils.DOCS_ROOTDIR.resolve("record-encryption-quickstart").resolve("index.adoc");
            var quickstarts = List.of(new Quickstart("record-encryption-quickstart(vault)", recordEncryptionQuickstart, blockIsInvariantOrMatches("kms", "vault")));
            return quickstarts.stream().map((q) -> extractCodeBlocks(blockExtractor, q));
        }
    }

    private static Predicate<StructuralNode> blockIsInvariantOrMatches(String variantKey, String variantValue) {
        return (sn) -> {
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

    @ParameterizedTest
    @MethodSource("quickstarts")
    void quickstart(List<Block> shellBlocks) {
        Path shellScript = writeShellScript(shellBlocks);

        executeScript(shellScript);
    }

    private void executeScript(Path shellScript) {
        var builder = new ProcessBuilder("/bin/sh", "-c", "source " + shellScript.toAbsolutePath().toString());

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
            var tempFile = Files.createTempFile("quickstart", ".sh");
            try (var writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8)) {
                writer.write("set -e -o pipefail");
                writer.newLine();
                shellBlocks.forEach(block -> {
                    try (var reader = new BufferedReader(new StringReader(block.content()))) {
                        String line;
                        writer.write("""
                                echo "##############"
                                echo "Code block source: %s (line %s)"
                                """.formatted(block.asciiDocFile(), block.lineNumber()));
                        writer.newLine();
                        while ((line = reader.readLine()) != null) {
                            // no support for line continuations
                            line = line.replaceAll("^\\$ *", ""); // chomp the shell prompt
                            writer.write(line);
                            writer.newLine();
                        }
                        writer.newLine();
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

    record Quickstart(String name, Path path, Predicate<StructuralNode> selector) {
    };
}
