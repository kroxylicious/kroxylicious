/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.kms;

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
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKmsFacadeException;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustStore;

import edu.umd.cs.findbugs.annotations.Nullable;

import static com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultContainerBuilder.lowkeyVault;

@SuppressWarnings("java:S112")
public class AzureKeyVaultKmsTestKmsFacade extends AbstractAzureKeyVaultKmsTestKmsFacade {

    public static final String TENANT_ID = "identity";
    public static final String KEY_VAULT_NAME = "default";

    @Nullable
    private LowkeyVaultContainer kms;
    @Nullable
    private OauthServerContainer oauthServer;

    protected AzureKeyVaultKmsTestKmsFacade() {
    }

    @Override
    public boolean isAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    public void startKms() {
        this.kms = startKeyVault();
        this.oauthServer = startMockOauthServer();
    }

    private static OauthServerContainer startMockOauthServer() {
        OauthServerContainer oauthServerContainer = new OauthServerContainer();
        oauthServerContainer.start();
        return oauthServerContainer;
    }

    public void stopKms() {
        if (kms != null) {
            kms.stop();
        }
        if (oauthServer != null) {
            oauthServer.stop();
        }
    }

    public static LowkeyVaultContainer startKeyVault() {
        String image = "nagyesta/lowkey-vault:5.0.14";
        final DockerImageName imageName = DockerImageName.parse("mirror.gcr.io/" + image)
                .asCompatibleSubstituteFor(DockerImageName.parse(image));
        final LowkeyVaultContainer lowkeyVaultContainer = lowkeyVault(imageName)
                .vaultNames(Set.of(KEY_VAULT_NAME))
                .build()
                .withImagePullPolicy(PullPolicy.defaultPolicy());
        lowkeyVaultContainer.start();
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
