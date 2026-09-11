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
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

import org.asciidoctor.Attributes;
import org.asciidoctor.ast.StructuralNode;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.doctools.asciidoc.Block;
import io.kroxylicious.doctools.asciidoc.BlockExtractor;
import io.kroxylicious.testing.integration.ShellUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Executes the shell commands found within quick start guides, in sequence, within a single shell.
 * Test will fail if any command fails.
 */
@EnabledIf("io.kroxylicious.doctools.validator.QuickStartDT#isEnvironmentValid")
@SuppressWarnings("java:S3577") // ignoring naming convention for the test class
class QuickStartDT {

    private static final Logger LOGGER = LoggerFactory.getLogger(QuickStartDT.class);
    private static final FileAttribute<Set<PosixFilePermission>> OWNER_RWX = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));

    private static List<Arguments> quickStarts() {
        Assertions.assertThat(Utils.OPERATOR_ZIP).exists();
        Assertions.assertThat(Utils.PROXY_IMAGE_TARBALL).exists();
        Assertions.assertThat(Utils.OPERATOR_IMAGE_TARBALL).exists();

        var attributes = Attributes.builder()
                .attribute("OperatorAssetZipLink", pathToFileUrl(Utils.OPERATOR_ZIP))
                .build();

        var recordEncryptionQuickstart = Utils.DOCS_ROOTDIR.resolve("record-encryption-quick-start").resolve("index.adoc");
        var quickStarts = List.of(new Quickstart("record-encryption-quick-start(vault)", recordEncryptionQuickstart, blockIsInvariantOrMatches("kms", "vault")),
                new Quickstart("record-encryption-quick-start(localstack)", recordEncryptionQuickstart, blockIsInvariantOrMatches("kms", "localstack")));
        return quickStarts.stream().map(q -> extractCodeBlocks(q, attributes)).toList();
    }

    @ParameterizedTest
    @MethodSource("quickStarts")
    void quickStart(List<Block> shellBlocks) {
        // One throw-away Minikube profile per @ParameterizedTest invocation (i.e. per KMS variant),
        // seeded with the locally built images. The quick start's
        // `export MINIKUBE_PROFILE=${MINIKUBE_PROFILE:-quickstart-${RANDOM}}` line reuses this value.
        var profile = "quickstart-" + ThreadLocalRandom.current().nextInt(100_000);
        try {
            ShellUtils.exec("minikube", "start", "-p", profile);
            loadImage(profile, Utils.PROXY_IMAGE_TARBALL, "kroxylicious/proxy");
            loadImage(profile, Utils.OPERATOR_IMAGE_TARBALL, "kroxylicious/operator");

            // Check 2: the quick start's kubectl commands follow the *active* context, not MINIKUBE_PROFILE.
            // If it is not our profile, the operator lands in a cluster without the images.
            boolean contextOk = ShellUtils.execValidate(lines -> lines.anyMatch(l -> l.trim().equals(profile)), lines -> true,
                    "kubectl", "config", "current-context");
            if (!contextOk) {
                throw new AssertionError("active kube-context is not '" + profile + "' before running the quick start (issue #4404)");
            }
            LOGGER.atInfo().addKeyValue("profile", profile).log("Quick start: kube-context confirmed, running shell script");

            var actual = executeScript(writeShellScript(shellBlocks), profile);
            try {
                assertThat(actual)
                        .succeedsWithin(Duration.ofMinutes(10))
                        .satisfies(er -> assertThat(er.exitValue())
                                .withFailMessage("Script failed - %s", er)
                                .isZero());
            }
            finally {
                if (!actual.isDone()) {
                    actual.cancel(true);
                }
            }
        }
        finally {
            try {
                ShellUtils.exec("minikube", "delete", "-p", profile);
            }
            catch (RuntimeException | AssertionError e) {
                // The quick start's own "minikube delete" cleanup step may already have removed it.
                LOGGER.atWarn().addKeyValue("profile", profile).setCause(e).log("Minikube profile cleanup failed");
            }
        }
    }

    /**
     * Loads a container-image tarball into {@code profile}. Delegates to {@code scripts/minikube-image-load.sh},
     * which fails (unlike a bare {@code minikube image load} — kubernetes/minikube#23471) if the load errored
     * or if {@code expectedRepo} (e.g. {@code kroxylicious/operator}) is not listed afterwards.
     */
    private static void loadImage(String profile, Path tarball, String expectedRepo) {
        ShellUtils.exec(Utils.SCRIPTS_DIR.resolve("minikube-image-load.sh").toString(), "--profile", profile, tarball.toString(), expectedRepo);
        LOGGER.atInfo().addKeyValue("profile", profile).addKeyValue("image", expectedRepo).log("Image loaded into Minikube profile");
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

    private static Arguments extractCodeBlocks(Quickstart qs, Attributes attributes) {
        try (var extractor = new BlockExtractor()) {
            extractor.withAttributes(attributes);
            var pred = isShellBlock().and(qs.selector());
            var cmds = extractor.extract(qs.path(), pred);
            return Arguments.argumentSet(qs.name(), cmds);
        }
    }

    private static Predicate<StructuralNode> isShellBlock() {
        return sn -> Objects.equals(sn.getAttribute("style", null), "source") &&
                Objects.equals(sn.getAttribute("language", null), "terminal");
    }

    private CompletableFuture<ExecutionResult> executeScript(Path shellScript, String minikubeProfile) {
        return CompletableFuture.supplyAsync(() -> {
            var builder = new ProcessBuilder(shellScript.toAbsolutePath().toString());
            builder.environment().put("MINIKUBE_PROFILE", minikubeProfile);

            try (var stdoutExecutor = Executors.newSingleThreadExecutor();
                    var stderrExecutor = Executors.newSingleThreadExecutor()) {
                var p = builder.start();
                return awaitScript(p, CompletableFuture.runAsync(() -> streamAndLog(p.getInputStream(), "stdout"), stdoutExecutor),
                        CompletableFuture.runAsync(() -> streamAndLog(p.getErrorStream(), "stderr"), stderrExecutor));
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to run script containing quick start commands: %s".formatted(shellScript), e);
            }
        });
    }

    private static ExecutionResult awaitScript(Process p, CompletableFuture<Void> readOutput, CompletableFuture<Void> readStderr) {
        try {
            try {
                p.getOutputStream().close();
                p.waitFor();
                readOutput.join();
                readStderr.join();
                return new ExecutionResult(p.exitValue(), null);
            }
            finally {
                p.destroy();
            }
        }
        catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            readOutput.join();
            readStderr.join();
            return new ExecutionResult(-1, e);
        }
    }

    private static void streamAndLog(InputStream inputStream, String streamName) {
        try (var reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty()) {
                    LOGGER.atInfo()
                            .addKeyValue("stream", streamName)
                            .log(line);
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path writeShellScript(List<Block> shellBlocks) {
        try {
            var tempFile = Files.createTempFile("quick-start", ".sh", OWNER_RWX);
            try (var writer = new PrintWriter(Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8))) {
                writer.println("#!/usr/bin/env bash");
                writer.println("set -e -v -o pipefail");
                shellBlocks.forEach(block -> {
                    try (var reader = new BufferedReader(new StringReader(block.content()))) {
                        writer.println("""
                                echo "##############"
                                echo "Executing script block from %s:%d"
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
        return ShellUtils.validateToolsOnPath("minikube");
    }

    private record ExecutionResult(int exitValue, Exception e) {}
}
