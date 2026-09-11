/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.azure;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.security.KeyStore;
import java.util.Set;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;

import com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultContainer;

import io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig;
import io.kroxylicious.kms.provider.azure.config.auth.Oauth2ClientCredentialsConfig;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.proxy.tag.VisibleForTesting;
import io.kroxylicious.testing.kms.TestKekManager;
import io.kroxylicious.testing.kms.TestKmsFacadeException;

import edu.umd.cs.findbugs.annotations.Nullable;

import static com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultContainerBuilder.lowkeyVault;

/**
 * An {@link AbstractAzureKeyVaultKmsTestKmsFacade} backed by containerised instances of
 * <a href="https://github.com/nagyesta/lowkey-vault">Lowkey Vault</a> (an Azure Key Vault
 * emulator) and a mock OAuth2 server, both run using Testcontainers.
 */
@SuppressWarnings("java:S112")
public class AzureKeyVaultKmsTestKmsFacade extends AbstractAzureKeyVaultKmsTestKmsFacade {

    /**
     * Tenant id used to authenticate with the mock OAuth2 server.
     */
    public static final String TENANT_ID = "identity";
    /**
     * Name of the vault created within the Lowkey Vault instance.
     */
    public static final String KEY_VAULT_NAME = "default";

    @Nullable
    private LowkeyVaultContainer kms;
    @Nullable
    private OauthServerContainer oauthServer;

    /**
     * Creates the facade.
     */
    protected AzureKeyVaultKmsTestKmsFacade() {
        // Intentionally empty
    }

    @Override
    public boolean isAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    @Override
    public void startKms() {
        this.kms = startKeyVault();
        this.oauthServer = startMockOauthServer();
    }

    private static OauthServerContainer startMockOauthServer() {
        OauthServerContainer oauthServerContainer = new OauthServerContainer();
        oauthServerContainer.start();
        return oauthServerContainer;
    }

    @Override
    public void stopKms() {
        if (kms != null) {
            kms.stop();
        }
        if (oauthServer != null) {
            oauthServer.stop();
        }
    }

    /**
     * Creates and starts a Lowkey Vault container.
     *
     * @return the started container
     */
    public static LowkeyVaultContainer startKeyVault() {
        final LowkeyVaultContainer lowkeyVaultContainer = createLowKeyContainer();
        lowkeyVaultContainer.start();
        return lowkeyVaultContainer;
    }

    @VisibleForTesting
    static LowkeyVaultContainer createLowKeyContainer() {
        String image = "nagyesta/lowkey-vault:7.3.74-ubi10-minimal@sha256:a1c0c0ddaecd7adeef30ee242fc3066ef3768b4f702a2a6b0409712294547b71";
        final DockerImageName imageName = DockerImageName.parse("mirror.gcr.io/" + image)
                .asCompatibleSubstituteFor(DockerImageName.parse(image.substring(0, image.indexOf("@"))));
        final LowkeyVaultContainer lowkeyVaultContainer = lowkeyVault(imageName)
                .vaultNames(Set.of(KEY_VAULT_NAME))
                .build()
                .withImagePullPolicy(PullPolicy.defaultPolicy());
        return lowkeyVaultContainer;
    }

    @SuppressWarnings("java:S5443") // this is test code, writing keys to public temp dir is intentional
    @Override
    public AzureKeyVaultConfig getKmsServiceConfig() {
        if (kms == null) {
            throw new IllegalStateException("kms is not initialized");
        }
        if (oauthServer == null) {
            throw new IllegalStateException("entraMock is not initialized");
        }
        URI defaultVaultBaseUrl = URI.create(kms.getDefaultVaultBaseUrl());
        try {
            KeyStore defaultKeyStore = kms.getDefaultKeyStore();
            File tempFile = File.createTempFile("lowkey-store", ".jks");
            try (FileOutputStream stream = new FileOutputStream(tempFile)) {
                kms.getDefaultKeyStore().store(stream, kms.getDefaultKeyStorePassword().toCharArray());
            }
            TrustStore vaultTrust = new TrustStore(tempFile.getAbsolutePath(), new InlinePassword(kms.getDefaultKeyStorePassword()), defaultKeyStore.getType());
            Tls vaultTls = new Tls(null, vaultTrust, null, null);
            TrustStore entraTrust = new TrustStore(oauthServer.getTrustStoreLocation(), new InlinePassword(oauthServer.getTrustStorePassword()),
                    oauthServer.getTrustStoreType());
            Tls entraTls = new Tls(null, entraTrust, null, null);
            return new AzureKeyVaultConfig(
                    new Oauth2ClientCredentialsConfig(oauthServer.getBaseUri(), TENANT_ID, new InlinePassword("abc"), new InlinePassword("def"),
                            URI.create("https://vault.azure.net/.default"),
                            entraTls),
                    null, KEY_VAULT_NAME, defaultVaultBaseUrl.getHost(), null, defaultVaultBaseUrl.getPort(), vaultTls);
        }
        catch (Exception e) {
            throw new TestKmsFacadeException(e);
        }
    }

    @Override
    public final TestKekManager getTestKekManager() {
        if (kms == null) {
            throw new IllegalStateException("kms is not initialized");
        }
        return new AzureKmsTestKekManager(kms.getEndpointAuthority(), kms.getDefaultVaultAuthority(), kms.getVaultBaseUrl(KEY_VAULT_NAME));
    }
}
