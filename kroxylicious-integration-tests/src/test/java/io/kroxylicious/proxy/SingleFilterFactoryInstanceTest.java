/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.test.tester.MockServerKroxyliciousTester;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;

@ExtendWith(NettyLeakDetectorExtension.class)
public class SingleFilterFactoryInstanceTest {
    public static final String INITIALISATION_COUNTER = "configInstanceId";
    private MockServerKroxyliciousTester mockTester;

    @AfterEach
    public void afterEach() {
        if (mockTester != null) {
            mockTester.close();
        }
    }

    @Test
    void shouldOnlyInitialiseFilterFactoryOnce() {
        // Given
        final UUID configInstance = UUID.randomUUID();

        // When
        mockTester = mockKafkaKroxyliciousTester(
                (mockBootstrap) -> proxy(mockBootstrap)
                                                       .addToFilters(
                                                               new FilterDefinitionBuilder("InvocationCountingFilterFactory").withConfig(
                                                                       INITIALISATION_COUNTER,
                                                                       configInstance
                                                               )
                                                                                                                             .build()
                                                       )
        );

        // Then
        InvocationCountingFilterFactory.assertInvocationCount(configInstance, 1);
    }

    @Test
    void shouldInitialiseOncePerConfig() {
        // Given
        final UUID configInstanceA = UUID.randomUUID();
        final UUID configInstanceB = UUID.randomUUID();

        // When
        mockTester = mockKafkaKroxyliciousTester(
                (mockBootstrap) -> proxy(mockBootstrap)
                                                       .addToFilters(
                                                               new FilterDefinitionBuilder("InvocationCountingFilterFactory").withConfig(
                                                                       INITIALISATION_COUNTER,
                                                                       configInstanceA
                                                               ).build()
                                                       )
                                                       .addToFilters(
                                                               new FilterDefinitionBuilder("InvocationCountingFilterFactory").withConfig(
                                                                       INITIALISATION_COUNTER,
                                                                       configInstanceB
                                                               ).build()
                                                       )
        );

        // Then
        InvocationCountingFilterFactory.assertInvocationCount(configInstanceA, 1);
        InvocationCountingFilterFactory.assertInvocationCount(configInstanceB, 1);
    }

}
