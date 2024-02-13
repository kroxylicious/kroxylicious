/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Set;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import edu.umd.cs.findbugs.annotations.NonNull;

public abstract class AbstractVaultTestKmsFacade implements TestKmsFacade<Config, String, VaultEdek> {

    protected static final String VAULT_ROOT_TOKEN = "rootToken";
    private String kmsVaultToken;

    protected AbstractVaultTestKmsFacade() {
    }

    protected abstract void startVault();

    protected abstract void stopVault();

    @Override
    public final void start() {

        startVault();

        // enable transit engine
        enableTransit();

        // create policy
        var policyName = "kroxylicious_encryption_filter_policy";
        try (var policyStream = AbstractVaultTestKmsFacade.class.getResourceAsStream("kroxylicious_encryption_filter_policy.hcl")) {
            createPolicy(policyName, policyStream);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create policy", e);
        }
        // create least-privilege vault token that KMS will use. The decision to use orphan tokens
        // (https://developer.hashicorp.com/vault/docs/concepts/tokens#token-hierarchies-and-orphan-tokens)
        // but no functional impact. A child token would work too.
        kmsVaultToken = createOrphanToken("kroxylicious_encryption_filter", true, Set.of(policyName));
    }

    protected abstract void enableTransit();

    protected abstract void createPolicy(String policyName, InputStream policyStream);

    protected abstract String createOrphanToken(String description, boolean noDefaultPolicy, Set<String> policies);

    @NonNull
    protected abstract URI getVaultUrl();

    @Override
    public final Config getKmsServiceConfig() {
        return new Config(getVaultTransitEngineUrl(), new InlinePassword(kmsVaultToken), null);
    }

    @Override
    public final Class<VaultKmsService> getKmsServiceClass() {
        return VaultKmsService.class;
    }

    @Override
    public final void stop() {
        stopVault();
    }

    @NonNull
    protected final URI getVaultTransitEngineUrl() {
        return getVaultUrl().resolve("v1/transit/");
    }

}
