/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkResultCollectorTest {

    @Nested
    @EnableKubernetesMockClient(crud = true)
    class PodDiscovery {

        KubernetesClient kubeClient;

        @Test
        void findsPodByLabel() {
            // given
            Pod pod = new PodBuilder()
                    .withNewMetadata()
                    .withName("omb-benchmark-abc123")
                    .withNamespace("kafka")
                    .addToLabels("app", "omb-benchmark")
                    .endMetadata()
                    .withNewSpec()
                    .endSpec()
                    .build();
            kubeClient.pods().inNamespace("kafka").resource(pod).create();

            BenchmarkResultCollector collector = new BenchmarkResultCollector(kubeClient, "kafka", "app=omb-benchmark");

            // when
            Pod found = collector.findBenchmarkPod();

            // then
            assertThat(found.getMetadata().getName()).isEqualTo("omb-benchmark-abc123");
        }

        @Test
        void throwsWhenNoPodFound() {
            // given - no pods created
            BenchmarkResultCollector collector = new BenchmarkResultCollector(kubeClient, "kafka", "app=omb-benchmark");

            // when/then
            assertThatThrownBy(collector::findBenchmarkPod)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no benchmark pod found");
        }
    }

    @Nested
    class TarExtraction {

        @TempDir
        Path outputDir;

        @Test
        void extractsJsonFilesFromTar() throws IOException {
            // given
            String resultJson = "{\"publishRate\": [50000.0]}";
            InputStream tarStream = createTarWithEntries(
                    new TarEntry("results/result-1.json", resultJson),
                    new TarEntry("results/result-2.json", "{\"publishRate\": [49000.0]}"));

            // when
            List<Path> extracted = BenchmarkResultCollector.extractTar(tarStream, outputDir);

            // then
            assertThat(extracted).hasSize(2);
            assertThat(outputDir.resolve("result-1.json")).exists().hasContent(resultJson);
            assertThat(outputDir.resolve("result-2.json")).exists().hasContent("{\"publishRate\": [49000.0]}");
        }

        @Test
        void skipsNonJsonFiles() throws IOException {
            // given
            InputStream tarStream = createTarWithEntries(
                    new TarEntry("results/result.json", "{\"data\": true}"),
                    new TarEntry("results/readme.txt", "not a result"));

            // when
            List<Path> extracted = BenchmarkResultCollector.extractTar(tarStream, outputDir);

            // then
            assertThat(extracted).hasSize(1);
            assertThat(outputDir.resolve("result.json")).exists();
            assertThat(outputDir.resolve("readme.txt")).doesNotExist();
        }

        @Test
        void skipsDirectoryEntries() throws IOException {
            // given
            InputStream tarStream = createTarWithEntries(
                    new TarEntry("results/", null),
                    new TarEntry("results/result.json", "{\"data\": true}"));

            // when
            List<Path> extracted = BenchmarkResultCollector.extractTar(tarStream, outputDir);

            // then
            assertThat(extracted).hasSize(1);
            assertThat(outputDir.resolve("result.json")).exists();
        }

        @Test
        void createsOutputDirectoryIfNeeded() throws IOException {
            // given
            Path nested = outputDir.resolve("sub/dir");
            InputStream tarStream = createTarWithEntries(
                    new TarEntry("results/result.json", "{\"data\": true}"));

            // when
            List<Path> extracted = BenchmarkResultCollector.extractTar(tarStream, nested);

            // then
            assertThat(extracted).hasSize(1);
            assertThat(nested.resolve("result.json")).exists();
        }

        private record TarEntry(String name, String content) {}

        private InputStream createTarWithEntries(TarEntry... entries) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (TarArchiveOutputStream tar = new TarArchiveOutputStream(baos)) {
                for (TarEntry entry : entries) {
                    TarArchiveEntry archiveEntry = new TarArchiveEntry(entry.name());
                    if (entry.content() != null) {
                        byte[] data = entry.content().getBytes(StandardCharsets.UTF_8);
                        archiveEntry.setSize(data.length);
                        tar.putArchiveEntry(archiveEntry);
                        tar.write(data);
                    }
                    else {
                        tar.putArchiveEntry(archiveEntry);
                    }
                    tar.closeArchiveEntry();
                }
            }
            return new ByteArrayInputStream(baos.toByteArray());
        }
    }
}
