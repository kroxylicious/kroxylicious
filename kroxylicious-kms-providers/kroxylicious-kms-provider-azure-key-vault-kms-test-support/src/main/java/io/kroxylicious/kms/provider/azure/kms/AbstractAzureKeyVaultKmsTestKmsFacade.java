/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.kms;

import java.time.Duration;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;

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

import io.kroxylicious.kms.provider.azure.AzureKeyVaultEdek;
import io.kroxylicious.kms.provider.azure.AzureKeyVaultKmsService;
import io.kroxylicious.kms.provider.azure.WrappingKey;
import io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.UnknownAliasException;

public abstract class AbstractAzureKeyVaultKmsTestKmsFacade implements TestKmsFacade<AzureKeyVaultConfig, WrappingKey, AzureKeyVaultEdek> {

    protected AbstractAzureKeyVaultKmsTestKmsFacade() {
    }

    protected abstract void startKms();

    protected abstract void stopKms();

    @Override
    public final void start() {
        startKms();
    }

    @Override
    public final Class<AzureKeyVaultKmsService> getKmsServiceClass() {
        return AzureKeyVaultKmsService.class;
    }

    @Override
    public final void stop() {
        stopKms();
    }

    public static class AzureKmsTestKekManager implements TestKekManager {
        KeyClient keyClient;

        public AzureKmsTestKekManager(String endpointAuthority, String defaultVaultAuthority, String vaultBaseUrl) {
            keyClient = new KeyClientBuilder().credential(new BasicAuthenticationCredential("abc", "def"))
                    .httpClient(createHttpClient(endpointAuthority, defaultVaultAuthority)).vaultUrl(vaultBaseUrl)
                    .disableChallengeResourceVerification().buildClient();
        }

        @Override
        public void generateKek(String alias) {
            String normalizedAlias = normalize(alias);
            // createKey succeeds idempotently, not sure if azure or lowkey behaviour
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
