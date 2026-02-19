/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

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
}
