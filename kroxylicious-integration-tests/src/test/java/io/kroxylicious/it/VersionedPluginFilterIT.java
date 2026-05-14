/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.Response;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class VersionedPluginFilterIT {

    // v1 higher than v2 because the tags must be ascending and v1 is the second filter applied
    private static final int V1_TAG = 101;
    private static final int V2_TAG = 100;

    @Test
    void testVersionBasedPluginDisambiguation(KafkaCluster cluster) {
        NamedFilterDefinition v1Filter = new NamedFilterDefinitionBuilder("versionedV1",
                "VersionedTestPlugin/v1")
                .withConfig("message", "from-v1")
                .withConfig("tag", V1_TAG)
                .build();

        NamedFilterDefinition v2Filter = new NamedFilterDefinitionBuilder("versionedV2",
                "VersionedTestPlugin/v2")
                .withConfig("tagValue", 42)
                .withConfig("tag", V2_TAG)
                .build();

        var config = proxy(cluster)
                .addToFilterDefinitions(v1Filter)
                .addToFilterDefinitions(v2Filter)
                .addToDefaultFilters(v1Filter.name())
                .addToDefaultFilters(v2Filter.name());

        try (var tester = kroxyliciousTester(config);
                var client = tester.simpleTestClient()) {
            ApiVersionsRequestData message = new ApiVersionsRequestData();
            Response response = client.getSync(new Request(API_VERSIONS, API_VERSIONS.latestVersion(), "client", message));

            assertThat(response).isNotNull();
            assertThat(response.payload().message().unknownTaggedFields())
                    .as("Both v1 (String) and v2 (int) filters should have added tagged fields")
                    .hasSize(2);

            var taggedFields = response.payload().message().unknownTaggedFields();
            var v1Tag = taggedFields.stream()
                    .filter(field -> field.tag() == V1_TAG && isStringTag(field))
                    .findFirst();
            var v2Tag = taggedFields.stream()
                    .filter(field -> field.tag() == V2_TAG && isIntTag(field))
                    .findFirst();

            assertThat(v1Tag)
                    .as("v1 filter should have added string tag")
                    .isPresent()
                    .hasValueSatisfying(field -> {
                        String value = new String(field.data(), StandardCharsets.UTF_8);
                        assertThat(value).isEqualTo("from-v1");
                    });

            assertThat(v2Tag)
                    .as("v2 filter should have added int tag")
                    .isPresent()
                    .hasValueSatisfying(field -> {
                        ByteBuffer buffer = ByteBuffer.wrap(field.data());
                        int value = buffer.getInt();
                        assertThat(value).isEqualTo(42);
                    });
        }
    }

    private boolean isStringTag(RawTaggedField field) {
        try {
            new String(field.data(), StandardCharsets.UTF_8);
            return field.data().length > 0 && field.data().length < 100;
        }
        catch (Exception e) {
            return false;
        }
    }

    private boolean isIntTag(RawTaggedField field) {
        return field.data().length == 4;
    }
}
