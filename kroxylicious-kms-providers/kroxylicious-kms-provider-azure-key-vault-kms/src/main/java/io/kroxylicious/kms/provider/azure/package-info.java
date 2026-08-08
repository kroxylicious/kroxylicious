/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * An implementation of the {@link io.kroxylicious.kms.service.Kms KMS service API} backed by
 * <a href="https://learn.microsoft.com/en-us/azure/key-vault/general/overview">Azure Key Vault</a>,
 * which wraps and unwraps Data Encryption Keys (DEKs) using Key Encryption Keys (KEKs) held in a vault.
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.kms.provider.azure;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;