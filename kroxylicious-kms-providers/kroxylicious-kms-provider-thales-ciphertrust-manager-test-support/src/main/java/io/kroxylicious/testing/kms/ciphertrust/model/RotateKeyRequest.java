/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.ciphertrust.model;

/**
 * Request for rotating a key in CipherTrust Manager.
 * <p>
 * This is an empty record that serializes to an empty JSON object ({}).
 * The CipherTrust Manager API requires a POST with an empty body for key rotation.
 * </p>
 */
public record RotateKeyRequest() {}
