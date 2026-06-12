/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * KMS provider implementation for Thales CipherTrust Manager.
 * <p>
 * CipherTrust Manager is an enterprise key management solution from Thales that provides
 * centralized cryptographic key lifecycle management. This provider implements envelope
 * encryption using CTM's primitive cryptographic operations (random, encrypt, decrypt).
 * </p>
 * <p>
 * Unlike AWS KMS or HashiCorp Vault, CTM doesn't natively support envelope encryption,
 * so this implementation manually generates DEKs and wraps them using CTM's encryption API.
 * </p>
 *
 * TODO:
 * * support domains parameter
 * * implement authentication using client auth (https://docs-cybersec.thalesgroup.com/bundle/v2.14-cdsp-cm/page/admin/cm_admin/authentication/rest-api/index.html)
 * * reject(or warn) anything that's not a v1 api?
 *
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.kms.provider.thales.ciphertrust;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;