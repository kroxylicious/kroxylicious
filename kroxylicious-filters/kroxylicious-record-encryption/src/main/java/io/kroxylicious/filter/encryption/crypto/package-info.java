/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * Encryption related code that's common to both {@link io.kroxylicious.filter.encryption.encrypt}
 * and {@link io.kroxylicious.filter.encryption.decrypt},
 * but doesn't directly handle key material (which is what {@link io.kroxylicious.filter.encryption.dek} is for).
 */
package io.kroxylicious.filter.encryption.crypto;
