/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates;

import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.ContainerBuilder;

import io.kroxylicious.systemtests.Constants;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.systemtests.TestTags.UNIT;
import static org.assertj.core.api.Assertions.assertThat;

@Tag(UNIT)
class ContainerTemplatesTest {

    public static final String CONTAINER_NAME = "test";

    @Test
    void baseImageBuilder() {
        ContainerBuilder test = ContainerTemplates.baseImageBuilder(CONTAINER_NAME, "test:1.2.3");
        assertThat(test.getName()).isEqualTo(CONTAINER_NAME);
        assertThat(test.getImage()).isEqualTo("test:1.2.3");
        assertThat(test.getImagePullPolicy()).isEqualTo(Constants.PULL_IMAGE_IF_NOT_PRESENT);
    }

    static Stream<String> imageContainingTagIsPulledOnce() {
        return Stream.of("latest",
                "LATEST",
                "a-latest",
                "latest-b",
                "a-latest-b",
                "snapshot",
                "SNAPSHOT",
                "a-snapshot",
                "snapshot-b",
                "a-snapshot-b");
    }

    private static @NonNull String randomImageWithTag(String tag) {
        return UUID.randomUUID() + ":" + tag;
    }

    /**
     * This is to help ensure images with latest tags are up-to-date for local testing. In CI the env will be clean.
     */
    @MethodSource
    @ParameterizedTest
    void imageContainingTagIsPulledOnce(String tag) {
        String image = randomImageWithTag(tag);
        ContainerBuilder test = ContainerTemplates.baseImageBuilder(CONTAINER_NAME, image);
        assertThat(test.getName()).isEqualTo(CONTAINER_NAME);
        assertThat(test.getImage()).isEqualTo(image);
        assertThat(test.getImagePullPolicy()).isEqualTo(Constants.PULL_IMAGE_ALWAYS);

        ContainerBuilder test2 = ContainerTemplates.baseImageBuilder(CONTAINER_NAME, image);
        assertThat(test2.getName()).isEqualTo(CONTAINER_NAME);
        assertThat(test2.getImage()).isEqualTo(image);
        assertThat(test2.getImagePullPolicy()).isEqualTo(Constants.PULL_IMAGE_IF_NOT_PRESENT);
    }
}
