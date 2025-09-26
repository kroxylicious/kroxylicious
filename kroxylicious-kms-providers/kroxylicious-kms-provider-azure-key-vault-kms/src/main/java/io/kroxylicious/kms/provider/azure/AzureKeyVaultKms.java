/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.random.RandomGenerator;

import javax.crypto.SecretKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kms.provider.azure.keyvault.GetKeyResponse;
import io.kroxylicious.kms.provider.azure.keyvault.JsonWebKey;
import io.kroxylicious.kms.provider.azure.keyvault.KeyVaultClient;
import io.kroxylicious.kms.provider.azure.keyvault.SupportedKeyType;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public class AzureKeyVaultKms implements Kms<WrappingKey, AzureKeyVaultEdek> {

    private static final Logger LOG = LoggerFactory.getLogger(AzureKeyVaultKms.class);

    private static final List<String> REQUIRED_OPERATIONS = List.of("wrapKey", "unwrapKey");
    private static final int AES_256_KEY_SIZE = 32;
    private static final AzureKeyVaultEdekSerde EDEK_SERDE = new AzureKeyVaultEdekSerde();
    private final KeyVaultClient client;
    private final String encryptingKeyVaultName;
    private final RandomGenerator random;

    public AzureKeyVaultKms(KeyVaultClient client, String encryptingKeyVaultName, RandomGenerator random) {
        this.client = client;
        this.encryptingKeyVaultName = encryptingKeyVaultName;
        this.random = random;
    }

    @Override
    public CompletionStage<DekPair<AzureKeyVaultEdek>> generateDekPair(WrappingKey wrappingKey) {
        LOG.debug("Generating dek pair for key version {}", wrappingKey);
        byte[] bytes = generateDek();
        CompletionStage<byte[]> wrap = client.wrap(wrappingKey, bytes);
        return wrap.handle((wrappedBytes, throwable) -> {
            if (isInstanceOfOrCompletionExceptionWithCauseSatisfying(throwable, UnexpectedHttpStatusCodeException.class, e -> e.getStatusCode() == 404)) {
                throw new UnknownKeyException("key '" + wrappingKey.keyName() + "' version '" + wrappingKey.keyVersion() + "' not found while attempting to wrap");
            }
            if (throwable != null) {
                throw new KmsException("request to wrap DEK with key " + wrappingKey + " failed", throwable);
            }
            else {
                return new DekPair<>(
                        new AzureKeyVaultEdek(wrappingKey.keyName(), wrappingKey.keyVersion(), wrappedBytes, wrappingKey.vaultName(), wrappingKey.supportedKeyType()),
                        recordEncryptionKey(bytes));
            }
        });
    }

    // todo use random bytes from KMS if it is Managed HSM, standard Key Vault we have to generate DEK proxy side
    private byte[] generateDek() {
        byte[] bytes = new byte[AES_256_KEY_SIZE];
        random.nextBytes(bytes);
        return bytes;
    }

    @NonNull
    private static SecretKey recordEncryptionKey(byte[] bytes) {
        // note that this algorithm is nothing to do with the KMS wrapping algorithm, this is required for the SecretKey
        // to be usable by the record encryption algorithms.
        return DestroyableRawSecretKey.takeOwnershipOf(bytes, "AES");
    }

    @Override
    public CompletionStage<SecretKey> decryptEdek(AzureKeyVaultEdek edek) {
        LOG.debug("Decrypting dek pair for key version {}", edek);
        return unwrap(edek);
    }

    static <T extends Throwable> boolean isInstanceOfOrCompletionExceptionWithCauseSatisfying(@Nullable Throwable t, Class<T> type, Predicate<T> predicate) {
        if (t == null) {
            return false;
        }
        if (type.isAssignableFrom(t.getClass())) {
            return predicate.test(type.cast(t));
        }
        else if (t instanceof CompletionException && t.getCause() != null && type.isAssignableFrom(t.getCause().getClass())) {
            return predicate.test(type.cast(t.getCause()));
        }
        else {
            return false;
        }
    }

    private CompletionStage<SecretKey> unwrap(AzureKeyVaultEdek edek) {
        return client.unwrap(new WrappingKey(edek.keyName(), edek.keyVersion(), edek.supportedKeyType(), edek.vaultName()), edek.edek())
                .handle((bytes, throwable) -> {
                    if (throwable != null) {
                        if (throwable instanceof UnexpectedHttpStatusCodeException e && e.getStatusCode() == 404
                                || throwable.getCause() instanceof UnexpectedHttpStatusCodeException ex && ex.getStatusCode() == 404) {
                            throw new UnknownKeyException("key not found, key name: '" + edek.keyName() + "' version '" + edek.keyVersion() + "'");
                        }
                        else {
                            throw new KmsException("failed to unwrap edek for key '" + edek.keyName() + "'", throwable);
                        }
                    }
                    else {
                        return recordEncryptionKey(bytes);
                    }
                });
    }

    @Override
    public Serde<AzureKeyVaultEdek> edekSerde() {
        return EDEK_SERDE;
    }

    /**
     * For Azure Key Vault we expect the alias to equal a key name.
     * @param alias The alias
     * @return a wrapping key for the alias
     * @throws UnknownAliasException if the named key was not found
     * @throws KmsException if there are any problems obtaining a key usable for wrapping, like unexpected
     * response codes, key attributes indicating the key is disabled, key attributes indicating wrapKey and
     * unwrapKey are not supported operations, and key types that are not supported like EC.
     */
    @Override
    public CompletionStage<WrappingKey> resolveAlias(String alias) {
        LOG.debug("Resolving alias {}", alias);
        return client.getKey(encryptingKeyVaultName, alias).handle((response, throwable) -> {
            if (throwable != null) {
                if (isInstanceOfOrCompletionExceptionWithCauseSatisfying(throwable, UnexpectedHttpStatusCodeException.class, e -> e.getStatusCode() == 404)) {
                    throw new UnknownAliasException(alias);
                }
                throw new KmsException("failed to check existence of '" + alias + "'", throwable);
            }
            else if (response == null) {
                throw new KmsException("get key returned null for: '" + alias + "'");
            }
            else {
                LOG.debug("resolved alias {} to {}", alias, response);
                JsonWebKey key = response.key();
                SupportedKeyType keyType = validateKeyAndExtractType(alias, response, key);
                return WrappingKey.parse(encryptingKeyVaultName, alias, response.key().keyId(), keyType);
            }
        });
    }

    @NonNull
    private static SupportedKeyType validateKeyAndExtractType(String alias, GetKeyResponse response, JsonWebKey key) {
        if (REQUIRED_OPERATIONS.stream().anyMatch(p -> !key.keyOperations().contains(p))) {
            throw new KmsException("key '" + alias + "' key_ops " + key.keyOperations() + " does not contain all of " + REQUIRED_OPERATIONS);
        }
        if (!response.attributes().enabled()) {
            throw new KmsException("key '" + alias + "' is not enabled");
        }
        Optional<SupportedKeyType> supportedKeyType = SupportedKeyType.fromKeyType(key.keyType());
        if (supportedKeyType.isEmpty()) {
            throw new KmsException("key '" + alias + "' has an unsupported type " + response.key().keyType());
        }
        return supportedKeyType.get();
    }

}
