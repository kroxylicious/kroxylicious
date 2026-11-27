/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.testclients;

import java.util.List;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;

import io.kroxylicious.systemtests.Constants;

import static org.assertj.core.api.Assertions.assertThat;

class TestClientsJobTemplatesTest {

    private static final String IMAGE = "quay.io/example/my-jar:1.0.0";
    private static final String JAR_PATH = "my.jar";

    private static JobBuilder producerJob() {
        // @formatter:off
        return new JobBuilder()
                .withNewSpec()
                    .withNewTemplate()
                        .withNewSpec()
                            .addNewContainer()
                                .withName("test-client-producer")
                                .withImage("test-clients:1.2.3")
                                .addNewEnv().withName("TOPIC").withValue("topic").endEnv()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec();
        // @formatter:on
    }

    @Test
    void withExtraClasspathImageMountsImageVolume() {
        // Given
        JobBuilder jobBuilder = producerJob();

        // When
        PodSpec podSpec = TestClientsJobTemplates.withExtraClasspathImage(jobBuilder, IMAGE, JAR_PATH)
                .build().getSpec().getTemplate().getSpec();

        // Then
        assertThat(podSpec.getVolumes())
                .singleElement()
                .satisfies(volume -> {
                    assertThat(volume.getImage().getReference()).isEqualTo(IMAGE);
                    assertThat(volume.getImage().getPullPolicy()).isEqualTo(Constants.PULL_IMAGE_IF_NOT_PRESENT);
                });
        Volume volume = podSpec.getVolumes().get(0);
        assertThat(podSpec.getContainers())
                .singleElement()
                .extracting(Container::getVolumeMounts)
                .asInstanceOf(InstanceOfAssertFactories.list(VolumeMount.class))
                .singleElement()
                .satisfies(mount -> {
                    assertThat(mount.getName()).isEqualTo(volume.getName());
                    assertThat(mount.getMountPath()).isEqualTo(Constants.TEST_CLIENTS_EXTRA_LIBS_DIR);
                    assertThat(mount.getReadOnly()).isTrue();
                });
    }

    @Test
    void withExtraClasspathImageSetsClasspathAndKeepsExistingEnv() {
        // Given
        JobBuilder jobBuilder = producerJob();
        List<EnvVar> originalEnv = jobBuilder.build().getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();

        // When
        List<EnvVar> env = TestClientsJobTemplates.withExtraClasspathImage(jobBuilder, IMAGE, JAR_PATH)
                .build().getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();

        // Then
        assertThat(env)
                .containsAll(originalEnv)
                .filteredOn(e -> e.getName().equals("CLASSPATH"))
                .singleElement()
                .extracting(EnvVar::getValue)
                .isEqualTo(Constants.TEST_CLIENTS_EXTRA_LIBS_DIR + "/" + JAR_PATH);
    }
}
