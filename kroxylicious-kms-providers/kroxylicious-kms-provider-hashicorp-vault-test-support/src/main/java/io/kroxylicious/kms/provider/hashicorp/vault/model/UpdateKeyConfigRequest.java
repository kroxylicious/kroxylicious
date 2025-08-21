/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record UpdateKeyConfigRequest(@JsonProperty("deletion_allowed") boolean deletionAllowed) {}
