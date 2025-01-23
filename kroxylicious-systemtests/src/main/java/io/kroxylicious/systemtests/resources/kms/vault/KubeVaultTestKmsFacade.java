/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.vault;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;

import io.kroxylicious.kms.provider.hashicorp.vault.AbstractVaultTestKmsFacade;
import io.kroxylicious.systemtests.installation.kms.vault.Vault;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * KMS Facade for Vault running inside Kube.
 */
public class KubeVaultTestKmsFacade extends AbstractVaultTestKmsFacade {
    private final Vault vault;

    /**
     * Instantiates a new Kube vault test kms facade.
     *
     */
    public KubeVaultTestKmsFacade() {
        this.vault = new Vault(VAULT_ROOT_TOKEN);
    }

    @Override
    public void startVault() {
        vault.deploy();
    }

    @Override
    public void stopVault() {
        try {
            vault.delete();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to delete Vault", e);
        }
    }

    @NonNull
    @Override
    protected URI getVaultUrl() {
        return vault.getVaultUrl();
    }
}
