/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms;

/**
 * Marker for {@link InClusterTestKmsFacadeFactory}s whose proxy authenticates with the KMS using static
 * credentials baked into the filter configuration. These are the facades exercised by {@code RecordEncryptionST}.
 */
public interface StaticCredentialAuthKmsFacadeFactory extends InClusterTestKmsFacadeFactory {
}
