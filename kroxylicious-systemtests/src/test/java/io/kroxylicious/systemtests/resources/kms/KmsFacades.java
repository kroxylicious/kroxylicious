/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms;

import java.util.function.Predicate;
import java.util.stream.Stream;

import io.kroxylicious.testing.kms.TestKmsFacade;
import io.kroxylicious.testing.kms.TestKmsFacadeFactory;

/**
 * Selects the {@link TestKmsFacade}s that run against a KMS deployed inside the test's Kubernetes cluster.
 */
public final class KmsFacades {

    private KmsFacades() {
    }

    /**
     * Discovers the in-cluster KMS facades matching the given predicate.
     * <p>
     * The systemtests module depends on the vault/aws/fortanix/azure {@code *-test-support} modules, which
     * register Testcontainers-based facades that a proxy pod running inside the cluster cannot reach. We
     * therefore keep only the facades whose factory lives in the systemtests module (its class name contains
     * {@code "systemtests"}) — those deploy the KMS into the cluster.
     * <p>
     * The returned stream is lazy: facades are built but neither checked for availability nor started, so
     * callers must apply their own {@link TestKmsFacade#isAvailable()} filter and {@link TestKmsFacade#start()}.
     *
     * @param predicate additional selection applied to the built facades
     * @return the matching facades
     */
    public static Stream<? extends TestKmsFacade<?, ?, ?>> inCluster(Predicate<? super TestKmsFacade<?, ?, ?>> predicate) {
        return TestKmsFacadeFactory.getTestKmsFacadeFactories()
                .filter(f -> f.getClass().getName().contains("systemtests"))
                .<TestKmsFacade<?, ?, ?>> map(TestKmsFacadeFactory::build)
                .filter(predicate);
    }
}
