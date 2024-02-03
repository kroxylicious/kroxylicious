/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.audit;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.kroxylicious.testing.kafka.junit5ext.Topic;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.proxy.filter.audit.Audit;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;

@ExtendWith(KafkaClusterExtension.class)
class AuditFilterIT {


    @Test
    void auditOne(KafkaCluster cluster, Topic auditLog) throws Exception {

        var builder = proxy(cluster);

        builder.addToFilters(buildEncryptionFilterDefinition(cluster, auditLog, Set.of(ApiKeys.CREATE_TOPICS)));

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin()) {
                admin.createTopics(List.of(new NewTopic("mytopic", Optional.empty(), Optional.empty()))).all().get(5, TimeUnit.SECONDS);
        }
    }

    private FilterDefinition buildEncryptionFilterDefinition(KafkaCluster auditCluster, Topic auditLog, Set<ApiKeys> keys) {
        var sinkConfig = Map.of("topic", auditLog.name(),
                "producerConfig", Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, auditCluster.getBootstrapServers()));
        return new FilterDefinitionBuilder(Audit.class.getSimpleName())
                .withConfig("apiKeys", keys)
                .withConfig("sink", "KafkaProducerEventSinkService")
                .withConfig("sinkConfig", sinkConfig)
                .build();
    }

}
