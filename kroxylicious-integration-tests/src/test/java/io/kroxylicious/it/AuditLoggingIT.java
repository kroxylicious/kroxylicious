/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import org.assertj.core.condition.AllOf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.AuditLoggingTestSupport.LogCaptor;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.it.AuditLoggingTestSupport.addAuditLogging;
import static io.kroxylicious.it.AuditLoggingTestSupport.auditAction;
import static io.kroxylicious.it.AuditLoggingTestSupport.isSuccess;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for audit logging functionality.
 * <p>
 * This test class validates that audit events are correctly logged via the Slf4j emitter.
 * Tests focus on audit logging in isolation, without metrics emitters.
 * </p>
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class AuditLoggingIT {

    /**
     * Tests that proxy lifecycle events (start and stop) are logged to Slf4j.
     * This test validates pure audit logging without metrics emitters.
     */
    @Test
    void shouldLogProxyStartAndStopToSlf4j(KafkaCluster cluster) {
        try (var captor = new LogCaptor()) {
            var builder = proxy(cluster);
            addAuditLogging(builder);

            // Proxy starts and stops within this try-with-resources block
            try (var tester = kroxyliciousTester(builder)) {
                // Proxy is now running
            }

            // Verify ProxyStart event was logged
            assertThat(captor.capturedEvents())
                    .haveExactly(1, AllOf.allOf(
                            auditAction("ProxyStart"),
                            isSuccess()));

            // Verify ProxyStop event was logged
            assertThat(captor.capturedEvents())
                    .haveExactly(1, AllOf.allOf(
                            auditAction("ProxyStop"),
                            isSuccess()));
        }
    }
}
