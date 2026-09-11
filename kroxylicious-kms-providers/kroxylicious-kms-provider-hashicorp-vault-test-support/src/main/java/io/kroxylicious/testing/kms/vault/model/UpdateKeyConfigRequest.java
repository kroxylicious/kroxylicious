/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.vault.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A request to update the configuration of a Transit engine key.
 *
 * @param deletionAllowed if true, the key is allowed to be deleted.
 * @see <a href="https://developer.hashicorp.com/vault/api-docs/secret/transit#update-key-configuration">Vault API Update Key Configuration</a>
 */
public record UpdateKeyConfigRequest(@JsonProperty("deletion_allowed") boolean deletionAllowed) {}
