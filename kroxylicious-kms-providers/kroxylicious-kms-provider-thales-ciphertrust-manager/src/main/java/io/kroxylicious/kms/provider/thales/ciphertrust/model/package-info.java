/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * HTTP request/response model classes for CipherTrust Manager REST API.
 * <p>
 * These models use Jackson annotations for JSON serialization/deserialization.
 * Binary fields (ciphertext, iv, tag, etc.) use {@code byte[]} and Jackson
 * automatically handles base64 encoding/decoding.
 * </p>
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.kms.provider.thales.ciphertrust.model;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;
