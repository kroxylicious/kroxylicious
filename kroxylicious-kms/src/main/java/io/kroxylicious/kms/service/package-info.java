/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * <p>Key Management System (KMS) API for managing Data Encryption Keys (DEKs) and Key Encryption Keys (KEKs).</p>
 * <p>The {@link io.kroxylicious.kms.service.Kms} interface provides methods for generating DEKs,
 * encrypting them with KEKs, and later decrypting encrypted DEKs (edeks) for use in record encryption.</p>
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.kms.service;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;