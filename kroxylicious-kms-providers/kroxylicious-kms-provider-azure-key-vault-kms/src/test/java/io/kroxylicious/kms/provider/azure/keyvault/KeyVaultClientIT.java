/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.keyvault;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;

import com.azure.security.keyvault.keys.KeyClient;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.models.KeyWrapAlgorithm;
import com.azure.security.keyvault.keys.cryptography.models.WrapResult;
import com.azure.security.keyvault.keys.models.CreateKeyOptions;
import com.azure.security.keyvault.keys.models.KeyOperation;
import com.azure.security.keyvault.keys.models.KeyType;
import com.azure.security.keyvault.keys.models.KeyVaultKey;
import com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultClientFactory;
import com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultContainer;

import io.kroxylicious.kms.provider.azure.WrappingKey;
import io.kroxylicious.kms.provider.azure.auth.BearerToken;
import io.kroxylicious.kms.provider.azure.auth.BearerTokenService;
import io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig;
import io.kroxylicious.kms.provider.azure.config.auth.EntraIdentityConfig;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.Tls;

import static com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultContainerBuilder.lowkeyVault;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@EnabledIf(value = "isDockerAvailable", disabledReason = "docker unavailable")
class KeyVaultClientIT {

    public static final String VAULT_NAME = "default";
    public static final Tls INSECURE_TLS = new Tls(null, new InsecureTls(true), null, null);
    public static final String KEY_NAME = "my-key";

    @Mock
    BearerTokenService bearerTokenService;

    LowkeyVaultContainer startVault() {
        String image = "nagyesta/lowkey-vault:4.0.67";
        final DockerImageName imageName = DockerImageName.parse("mirror.gcr.io/" + image)
                .asCompatibleSubstituteFor(DockerImageName.parse(image));
        final LowkeyVaultContainer lowkeyVaultContainer = lowkeyVault(imageName)
                .vaultNames(Set.of(VAULT_NAME))
                .build()
                .withImagePullPolicy(PullPolicy.defaultPolicy());
        lowkeyVaultContainer.start();
        return lowkeyVaultContainer;
    }

    @Test
    void getKey() {
        try (LowkeyVaultContainer lowkeyVaultContainer = startVault()) {
            BearerToken token = new BearerToken("mytoken", Instant.MIN, Instant.MAX);
            when(bearerTokenService.getBearerToken()).thenReturn(CompletableFuture.completedFuture(token));
            EntraIdentityConfig unused = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
            URI baseUri = URI.create(lowkeyVaultContainer.getDefaultVaultBaseUrl());
            KeyVaultClient keyVaultClient = new KeyVaultClient(bearerTokenService,
                    getAzureKeyVaultConfig(unused, baseUri));
            KeyClient keyClient = new LowkeyVaultClientFactory(lowkeyVaultContainer).getKeyClientBuilderFor(VAULT_NAME).buildClient();
            keyClient.createKey(new CreateKeyOptions(KEY_NAME, KeyType.RSA).setKeyOperations(KeyOperation.UNWRAP_KEY, KeyOperation.WRAP_KEY));
            assertThat(keyVaultClient.getKey(VAULT_NAME, KEY_NAME).toCompletableFuture()).succeedsWithin(Duration.ofSeconds(10)).satisfies(getKeyResponse -> {
                assertThat(getKeyResponse).isNotNull();
                assertThat(getKeyResponse.attributes().enabled()).isTrue();
                assertThat(getKeyResponse.key().keyId()).isNotBlank();
                assertThat(getKeyResponse.key().keyType()).isEqualTo("RSA");
                assertThat(getKeyResponse.key().keyOperations()).contains("wrapKey", "unwrapKey");
            });
        }
    }

    @Test
    void wrap() {
        try (LowkeyVaultContainer lowkeyVaultContainer = startVault()) {
            // given
            BearerToken token = new BearerToken("mytoken", Instant.MIN, Instant.MAX);
            when(bearerTokenService.getBearerToken()).thenReturn(CompletableFuture.completedFuture(token));
            EntraIdentityConfig unused = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
            URI baseUri = URI.create(lowkeyVaultContainer.getDefaultVaultBaseUrl());
            KeyVaultClient keyVaultClient = new KeyVaultClient(bearerTokenService,
                    getAzureKeyVaultConfig(unused, baseUri));
            KeyClient keyClient = new LowkeyVaultClientFactory(lowkeyVaultContainer).getKeyClientBuilderFor(VAULT_NAME).buildClient();
            KeyVaultKey key = keyClient
                    .createKey(new CreateKeyOptions(KEY_NAME, KeyType.RSA).setKeyOperations(KeyOperation.UNWRAP_KEY, KeyOperation.WRAP_KEY, KeyOperation.ENCRYPT));
            WrappingKey wrappingKey = WrappingKey.parse(VAULT_NAME, key.getName(), key.getId(), SupportedKeyType.RSA);
            byte[] dek = { 1, 2, 3 };

            // when
            CompletionStage<byte[]> wrapStage = keyVaultClient.wrap(wrappingKey, dek);

            // then
            assertThat(wrapStage.toCompletableFuture()).succeedsWithin(Duration.ofSeconds(10)).satisfies(wrapResponse -> {
                assertThat(wrapResponse).isNotNull();
                assertThat(wrapResponse).isNotEmpty();
            });
        }
    }

    private static AzureKeyVaultConfig getAzureKeyVaultConfig(EntraIdentityConfig unused, URI baseUri) {
        return new AzureKeyVaultConfig(unused, VAULT_NAME, baseUri.getHost(), null, baseUri.getPort(), INSECURE_TLS);
    }

    @Test
    void unwrap() {
        try (LowkeyVaultContainer lowkeyVaultContainer = startVault()) {
            // given
            BearerToken token = new BearerToken("mytoken", Instant.MIN, Instant.MAX);
            when(bearerTokenService.getBearerToken()).thenReturn(CompletableFuture.completedFuture(token));
            EntraIdentityConfig unused = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
            URI baseUri = URI.create(lowkeyVaultContainer.getDefaultVaultBaseUrl());
            KeyVaultClient keyVaultClient = new KeyVaultClient(bearerTokenService,
                    getAzureKeyVaultConfig(unused, baseUri));
            LowkeyVaultClientFactory lowkeyVaultClientFactory = new LowkeyVaultClientFactory(lowkeyVaultContainer);
            KeyClient keyClient = lowkeyVaultClientFactory.getKeyClientBuilderFor(VAULT_NAME).buildClient();
            KeyVaultKey key = keyClient.createKey(new CreateKeyOptions(KEY_NAME, KeyType.RSA).setKeyOperations(KeyOperation.UNWRAP_KEY, KeyOperation.WRAP_KEY,
                    KeyOperation.ENCRYPT, KeyOperation.DECRYPT));
            CryptographyClient cryptographyClient = lowkeyVaultClientFactory.getCryptoClientBuilderFor(VAULT_NAME).keyIdentifier(key.getId()).buildClient();
            byte[] dek = { 1, 2, 3 };
            WrapResult wrapResult = cryptographyClient.wrapKey(KeyWrapAlgorithm.RSA_OAEP_256, dek);
            WrappingKey wrappingKey = WrappingKey.parse(VAULT_NAME, key.getName(), key.getId(), SupportedKeyType.RSA);

            // when
            CompletionStage<byte[]> unwrapStage = keyVaultClient.unwrap(wrappingKey, wrapResult.getEncryptedKey());

            // then
            assertThat(unwrapStage.toCompletableFuture()).succeedsWithin(Duration.ofSeconds(10)).satisfies(wrapResponse -> {
                assertThat(wrapResponse).isNotNull();
                assertThat(wrapResponse).containsExactly(dek);
            });
        }
    }

    static boolean isDockerAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }
}
