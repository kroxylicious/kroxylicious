/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import static org.assertj.core.api.Assertions.assertThat;

class NameBuilderTest {

    @Test
    void appendsPartsInOrder() {
        // Given
        NameBuilder builder = new NameBuilder(63);

        // When
        String name = builder.append("foo").append('-').append("bar").toString();

        // Then
        assertThat(name).isEqualTo("foo-bar");
    }

    @Test
    void appendsNameOfResource() {
        // Given
        NameBuilder builder = new NameBuilder(63);
        Service service = new ServiceBuilder().withNewMetadata().withName("my-service").endMetadata().build();

        // When
        String name = builder.append("prefix-").appendNameOf(service).toString();

        // Then
        assertThat(name).isEqualTo("prefix-my-service");
    }

    @Test
    void appendNameOfResourceWithNoMetadataAppendsNothing() {
        // Given
        NameBuilder builder = new NameBuilder(63);
        Service service = new ServiceBuilder().build();

        // When
        String name = builder.append("prefix").appendNameOf(service).toString();

        // Then
        assertThat(name).isEqualTo("prefix");
    }

    @Test
    void appendNameOfResourceWithNoNameAppendsNothing() {
        // Given
        NameBuilder builder = new NameBuilder(63);
        Service service = new ServiceBuilder().withNewMetadata().endMetadata().build();

        // When
        String name = builder.append("prefix").appendNameOf(service).toString();

        // Then
        assertThat(name).isEqualTo("prefix");
    }

    @Test
    void buildReturnsBufferUnchangedWhenWithinMaxLength() {
        // Given
        NameBuilder builder = new NameBuilder(10);

        // When
        String name = builder.append("0123456789").toString();

        // Then
        assertThat(name).isEqualTo("0123456789");
    }

    @Test
    void buildTruncatesAndHashesWhenExceedingMaxLength() {
        // Given
        NameBuilder builder = new NameBuilder(20);

        // When
        String name = builder.append("012345678901234567890123456789").toString();

        // Then
        assertThat(name)
                .isEqualTo("012345678-585f8d7fd7");
    }

    @Test
    void buildIsDeterministicForTheSameInput() {
        // Given
        String overLongPart = "a-very-long-name-that-exceeds-the-maximum-permitted-length";

        // When
        String first = new NameBuilder(20).append(overLongPart).toString();
        String second = new NameBuilder(20).append(overLongPart).toString();

        // Then
        assertThat(first).isEqualTo(second);
    }

    @Test
    void buildProducesDifferentHashesForDifferentOverLongInputs() {
        // Given
        NameBuilder builderA = new NameBuilder(20).append("a-very-long-name-that-exceeds-the-maximum-permitted-length");
        NameBuilder builderB = new NameBuilder(20).append("another-very-long-name-that-exceeds-the-maximum-permitted-length");

        // When
        String nameA = builderA.toString();
        String nameB = builderB.toString();

        // Then
        assertThat(nameA).isNotEqualTo(nameB);
    }

    @Test
    void forRfc1035LabelNameUsesSixtyThreeCharacterLimit() {
        // Given
        String sixtyThreeChars = "a".repeat(63);

        // When
        String name = NameBuilder.forRfc1035LabelName().append(sixtyThreeChars).toString();

        // Then
        assertThat(name).isEqualTo(sixtyThreeChars);
    }

    @Test
    void forRfc1035LabelNameTruncatesOverSixtyThreeCharacters() {
        // Given
        String tooLong = "a".repeat(64);

        // When
        String name = NameBuilder.forRfc1035LabelName().append(tooLong).toString();

        // Then
        assertThat(name).hasSize(63);
    }

    @Test
    void forRfc1123LabelNameUsesSixtyThreeCharacterLimit() {
        // Given
        String sixtyThreeChars = "a".repeat(63);

        // When
        String name = NameBuilder.forRfc1123LabelName().append(sixtyThreeChars).toString();

        // Then
        assertThat(name).isEqualTo(sixtyThreeChars);
    }

    @Test
    void forDnsSubdomainNameUsesTwoHundredAndFiftyThreeCharacterLimit() {
        // Given
        String twoFiftyThreeChars = "a".repeat(253);

        // When
        String name = NameBuilder.forDnsSubdomainName().append(twoFiftyThreeChars).toString();

        // Then
        assertThat(name).isEqualTo(twoFiftyThreeChars);
    }

    @Test
    void forDnsSubdomainNameTruncatesOverTwoHundredAndFiftyThreeCharacters() {
        // Given
        String tooLong = "a".repeat(254);

        // When
        String name = NameBuilder.forDnsSubdomainName().append(tooLong).toString();

        // Then
        assertThat(name).hasSize(253);
    }

    @Test
    void forResourceUsesRfc1035LabelLimitForService() {
        // Given
        String tooLong = "a".repeat(64);

        // When
        String name = NameBuilder.forResource(Service.class).append(tooLong).toString();

        // Then
        assertThat(name).hasSize(63);
    }

    @Test
    void forResourceUsesRfc1123LabelLimitForNamespace() {
        // Given
        String tooLong = "a".repeat(64);

        // When
        String name = NameBuilder.forResource(Namespace.class).append(tooLong).toString();

        // Then
        assertThat(name).hasSize(63);
    }

    @Test
    void forResourceUsesDnsSubdomainLimitForOtherResourceKinds() {
        // Given
        String twoFiftyThreeChars = "a".repeat(253);

        // When
        String name = NameBuilder.forResource(ConfigMap.class).append(twoFiftyThreeChars).toString();

        // Then
        assertThat(name).isEqualTo(twoFiftyThreeChars);
    }

    @Test
    void forResourceUsesDnsSubdomainLimitForUnknownHasMetadataSubtypes() {
        // Given
        class CustomResource implements HasMetadata {
            @Override
            public io.fabric8.kubernetes.api.model.ObjectMeta getMetadata() {
                return null;
            }

            @Override
            public void setMetadata(io.fabric8.kubernetes.api.model.ObjectMeta metadata) {
                // unused
            }

            @Override
            public String getApiVersion() {
                return null;
            }

            @Override
            public void setApiVersion(String version) {
                // unused
            }

            @Override
            public String getKind() {
                return null;
            }
        }
        String tooLongForLabel = "a".repeat(64);

        // When
        String name = NameBuilder.forResource(CustomResource.class).append(tooLongForLabel).toString();

        // Then
        assertThat(name).isEqualTo(tooLongForLabel);
    }

}
