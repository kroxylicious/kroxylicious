/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

class MdsMetricsTest extends MdsReauthenticationTestSupport {
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    @BeforeEach
    void register() {
        Metrics.addRegistry(registry);
    }

    @AfterEach
    void unregister() {
        Metrics.removeRegistry(registry);
        registry.close();
    }

    @Test
    void recordsAcquisitionRenewalAndOneClosePerConnection() {
        // Given
        token("old", 30);
        success(30000);
        request().join();
        token("fresh", 60);
        success(30000);
        time(25);
        request().join();
        time(50);
        tokens.add(CompletableFuture.failedStage(new IOException("sensitive")));

        // When
        request().join();
        request().join();

        // Then
        assertThat(registry.get("kroxylicious.mds.token.duration").tag("outcome", "success").timer().count()).isEqualTo(2);
        assertThat(registry.get("kroxylicious.mds.token.duration").tag("outcome", "failure").timer().count()).isEqualTo(1);
        assertThat(registry.get("kroxylicious.mds.authentication").tags("phase", "renewal", "outcome", "success").counter().count()).isEqualTo(1);
        assertThat(registry.get("kroxylicious.mds.authentication").tags("phase", "renewal", "outcome", "failure").counter().count()).isEqualTo(1);
        assertThat(registry.get("kroxylicious.mds.connections.closed").tag("reason", "mds_io").counter().count()).isEqualTo(1);
        assertThat(registry.getMeters()).isNotEmpty().allSatisfy(meter -> assertThat(meter.getId().getTags())
                .isNotEmpty().allSatisfy(tag -> assertThat(tag.getKey()).isIn("virtual_cluster", "outcome", "phase", "reason")));
    }

    @Test
    void distinguishesLocalBackoffFromAnHttpRequest() {
        // Given
        tokens.add(CompletableFuture.failedStage(new MdsFailure(MdsFailure.Reason.MDS_BACKOFF)));

        // When
        request().join();

        // Then
        assertThat(registry.get("kroxylicious.mds.requests.rejected").tag("reason", "mds_backoff").counter().count()).isEqualTo(1);
        assertThat(registry.find("kroxylicious.mds.token.duration").timers()).allSatisfy(timer -> assertThat(timer.count()).isZero());
    }
}
