/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.ciphertrust.model;

/**
 * Request for rotating a key in CipherTrust Manager.
 * <p>
 * This record serializes to an empty JSON object ({}), which is what the
 * CipherTrust Manager API expects for key rotation.
 * </p>
 */
@SuppressWarnings("java:S2094") // Intentionally empty object
public record RotateKeyRequest() {}
