/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.keyvault;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The attributes of an Azure Key Vault key that are used by the KMS provider.
 *
 * @param enabled whether the key is enabled and can be used for cryptographic operations.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record KeyAttributes(@JsonProperty(required = true) boolean enabled) {}
