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
import java.time.Duration;
import java.util.Set;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;

import com.azure.core.credential.BasicAuthenticationCredential;
import com.azure.core.exception.ResourceNotFoundException;
import com.azure.core.http.HttpClient;
import com.azure.core.util.polling.SyncPoller;
import com.azure.security.keyvault.keys.KeyClient;
import com.azure.security.keyvault.keys.KeyClientBuilder;
import com.azure.security.keyvault.keys.models.CreateKeyOptions;
import com.azure.security.keyvault.keys.models.DeletedKey;
import com.azure.security.keyvault.keys.models.KeyOperation;
import com.azure.security.keyvault.keys.models.KeyType;
import com.azure.security.keyvault.keys.models.KeyVaultKey;
import com.github.nagyesta.lowkeyvault.http.ApacheHttpClient;
import com.github.nagyesta.lowkeyvault.http.AuthorityOverrideFunction;
import com.github.nagyesta.lowkeyvault.http.management.LowkeyVaultException;
import com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultContainer;

import io.kroxylicious.kms.provider.azure.AzureKeyVaultEdek;
import io.kroxylicious.kms.provider.azure.AzureKeyVaultKmsService;
import io.kroxylicious.kms.provider.azure.WrappingKey;
import io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig;
import io.kroxylicious.kms.provider.azure.config.auth.EntraIdentityConfig;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.TestKmsFacadeException;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustStore;

import edu.umd.cs.findbugs.annotations.Nullable;

import static com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultContainerBuilder.lowkeyVault;

@SuppressWarnings("java:S112")
public class AzureKeyVaultKmsTestKmsFacade implements TestKmsFacade<AzureKeyVaultConfig, WrappingKey, AzureKeyVaultEdek> {

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
        String image = "nagyesta/lowkey-vault:4.0.67";
        final DockerImageName imageName = DockerImageName.parse("mirror.gcr.io/" + image)
                .asCompatibleSubstituteFor(DockerImageName.parse(image));
        final LowkeyVaultContainer lowkeyVaultContainer = lowkeyVault(imageName)
                .vaultNames(Set.of(KEY_VAULT_NAME))
                .build()
                .withImagePullPolicy(PullPolicy.defaultPolicy());
        lowkeyVaultContainer.start();
        return lowkeyVaultContainer;
    }

    @Override
    public final void start() {
        startKms();
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
                    new EntraIdentityConfig(oauthServer.getBaseUri(), TENANT_ID, new InlinePassword("abc"), new InlinePassword("def"), null,
                            entraTls),
                    KEY_VAULT_NAME, defaultVaultBaseUrl.getHost(), null, defaultVaultBaseUrl.getPort(), vaultTls);
        }
        catch (Exception e) {
            throw new TestKmsFacadeException(e);
        }
    }

    @Override
    public final Class<AzureKeyVaultKmsService> getKmsServiceClass() {
        return AzureKeyVaultKmsService.class;
    }

    @Override
    public final void stop() {
        stopKms();
    }

    @Override
    public final TestKekManager getTestKekManager() {
        if (kms == null) {
            throw new IllegalStateException("kms is not initialized");
        }
        return new AzureKmsTestKekManager(kms);
    }

    private static class AzureKmsTestKekManager implements TestKekManager {
        KeyClient keyClient;

        private AzureKmsTestKekManager(LowkeyVaultContainer kms) {
            keyClient = new KeyClientBuilder().credential(new BasicAuthenticationCredential("abc", "def"))
                    .httpClient(createHttpClient(kms.getEndpointAuthority(), kms.getDefaultVaultAuthority())).vaultUrl(kms.getVaultBaseUrl(KEY_VAULT_NAME))
                    .disableChallengeResourceVerification().buildClient();
        }

        @Override
        public void generateKek(String alias) {
            String normalizedAlias = normalize(alias);
            // createKey succeeds imdempotently, not sure if azure or lowkey behaviour
            try {
                read(alias);
                throw new IllegalStateException("key '" + normalizedAlias + "' already exists");
            }
            catch (UnknownAliasException e) {
                keyClient.createKey(
                        new CreateKeyOptions(normalizedAlias, KeyType.RSA).setKeyOperations(KeyOperation.ENCRYPT, KeyOperation.DECRYPT, KeyOperation.WRAP_KEY,
                                KeyOperation.UNWRAP_KEY));
            }
        }

        @Override
        public KeyVaultKey read(String alias) {
            String normalizedAlias = normalize(alias);
            try {
                return keyClient.getKey(normalizedAlias);
            }
            catch (ResourceNotFoundException e) {
                throw new UnknownAliasException(normalizedAlias);
            }
        }

        @Override
        public void deleteKek(String alias) {
            String normalizedAlias = normalize(alias);
            try {
                SyncPoller<DeletedKey, Void> poller = keyClient.beginDeleteKey(normalizedAlias);
                poller.waitForCompletion(Duration.ofSeconds(10));
            }
            catch (ResourceNotFoundException e) {
                throw new UnknownAliasException(normalizedAlias);
            }
        }

        @Override
        public void rotateKek(String alias) {
            String normalizedAlias = normalize(alias);
            try {
                keyClient.rotateKey(normalizedAlias);
            }
            catch (ResourceNotFoundException e) {
                throw new UnknownAliasException(normalizedAlias);
            }
        }

        // the existing tests expect topic names containing _ can be used as KEK
        private static String normalize(String alias) {
            return alias.replace("_", "-");
        }

        private static HttpClient createHttpClient(String clientAuthority, String containerAuthority) {
            try {
                SSLContextBuilder builder = new SSLContextBuilder();
                builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
                return new ApacheHttpClient(new AuthorityOverrideFunction(clientAuthority, containerAuthority), new TrustSelfSignedStrategy(),
                        new DefaultHostnameVerifier());
            }
            catch (Exception e) {
                throw new LowkeyVaultException("Failed to create HTTP client.", e);
            }
        }
    }

}
