/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms;

/**
 * Marker for {@code TestKmsFacadeFactory}s whose KMS is deployed inside the test's Kubernetes cluster
 * (as opposed to the Testcontainers-based facades registered by the {@code *-test-support} modules, which a
 * proxy pod running in the cluster cannot reach).
 */
public interface InClusterTestKmsFacadeFactory {
}
