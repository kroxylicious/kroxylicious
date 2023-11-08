/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import javax.crypto.SecretKey;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A Data Encryption Key as both plaintext and encrypted.
 * @param edek The encrypted DEK.
 * @param dek The plaintext DEK.
 * @param <E> The type of encrypted DEK.
 */
public record DekPair<E>(@NonNull E edek, @NonNull SecretKey dek) {}
