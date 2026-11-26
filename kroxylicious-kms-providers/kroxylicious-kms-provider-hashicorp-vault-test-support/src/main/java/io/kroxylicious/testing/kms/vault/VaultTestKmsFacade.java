/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.vault;

import java.net.URI;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.vault.VaultContainer;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link io.kroxylicious.testing.kms.TestKmsFacade} backed by a HashiCorp Vault instance
 * provided by a <a href="https://hub.docker.com/r/hashicorp/vault">Vault</a> container.
 */
public class VaultTestKmsFacade extends AbstractVaultTestKmsFacade {
    private static final String IMAGE = "hashicorp/vault:2.2.2@sha256:dfa48c2fa2b0099ccb47681ec5c4804bf5aec9d6879ecfee07ad4b92a5427ad6";
    private static final DockerImageName HASHICORP_VAULT = DockerImageName.parse(IMAGE)
            .asCompatibleSubstituteFor(DockerImageName.parse(IMAGE.substring(0, IMAGE.indexOf("@"))));

    @SuppressWarnings("rawtypes")
    private @Nullable VaultContainer vaultContainer;

    /**
     * Creates the facade.
     */
    public VaultTestKmsFacade() {
        // Intentionally empty
    }

    @Override
    public boolean isAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    @Override
    @SuppressWarnings("resource")
    public void startVault() {
        vaultContainer = new VaultContainer<>(HASHICORP_VAULT).withVaultToken(VAULT_ROOT_TOKEN)
                .withEnv("SKIP_SETCAP", "true"); // Workaround for Vault 2.x in rootless containers (SETFCAP capability unavailable). Acceptable for development/testing.
        vaultContainer.start();
    }

    @Override
    public void stopVault() {
        if (vaultContainer != null) {
            vaultContainer.close();
        }
    }

    @Override
    protected URI getVaultUrl() {
        return URI.create(vaultContainer.getHttpHostAddress());
    }
}
