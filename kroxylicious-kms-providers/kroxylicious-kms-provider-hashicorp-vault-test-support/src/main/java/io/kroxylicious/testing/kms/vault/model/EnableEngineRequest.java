/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.vault.model;

/**
 * A request to enable a Vault secrets engine.
 *
 * @param type type of the secrets engine to enable (e.g. {@code transit}).
 * @see <a href="https://developer.hashicorp.com/vault/api-docs/system/mounts#enable-secrets-engine">Vault API Enable Secrets Engine</a>
 */
public record EnableEngineRequest(String type) {}
