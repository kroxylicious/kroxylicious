/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure;

import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.random.RandomGenerator;
import java.util.stream.Stream;

import javax.crypto.SecretKey;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.kms.provider.azure.keyvault.GetKeyResponse;
import io.kroxylicious.kms.provider.azure.keyvault.JsonWebKey;
import io.kroxylicious.kms.provider.azure.keyvault.KeyAttributes;
import io.kroxylicious.kms.provider.azure.keyvault.KeyVaultClient;
import io.kroxylicious.kms.provider.azure.keyvault.SupportedKeyType;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AzureKeyVaultKmsTest {

    public static final String KEY_NAME = "my-key";
    public static final List<String> REQUIRED_KEY_OPS = List.of("wrapKey", "unwrapKey");
    public static final byte[] WRAPPED_DEK_BYTES = { 1, 2, 3 };
    // 32 byte DEK
    public static final byte[] DEK_BYTES = Base64.getDecoder().decode("y6f9E5vVqJOFc5B/Rhp5v5V54/1AsLq11AxlurL6qNA=");
    public static final String VAULT_NAME = "myvault";
    public static final SupportedKeyType SUPPORTED_KEY_TYPE = SupportedKeyType.OCT;

    @Mock
    private KeyVaultClient keyVaultClient;
    @Mock
    private RandomGenerator randomGenerator;

    AzureKeyVaultKms azureKeyVaultKms;

    private static final String KEY_VERSION = "78deebed173b48e48f55abf87ed4cf71";
    private static final String KEY_ID = "https://myvault.vault.azure.net/keys/" + KEY_NAME + "/" + KEY_VERSION;

    @BeforeEach
    void setup() {
        azureKeyVaultKms = new AzureKeyVaultKms(keyVaultClient, VAULT_NAME, randomGenerator);
    }

    @CsvSource({ "RSA,RSA", "RSA-HSM,RSA_HSM", "oct,OCT", "oct-HSM,OCT_HSM" })
    @ParameterizedTest
    void resolveAliasSuccess(String keyType, SupportedKeyType expectedSupportedKeyType) {
        // given
        when(keyVaultClient.getKey(VAULT_NAME, KEY_NAME)).thenReturn(
                CompletableFuture.completedFuture(new GetKeyResponse(new JsonWebKey(KEY_ID, keyType, REQUIRED_KEY_OPS), new KeyAttributes(true))));
        // when
        CompletionStage<WrappingKey> stage = azureKeyVaultKms.resolveAlias(KEY_NAME);
        // then
        assertThat(stage.toCompletableFuture()).succeedsWithin(Duration.ZERO).satisfies(keyVersion -> {
            assertThat(keyVersion.supportedKeyType()).isEqualTo(expectedSupportedKeyType);
            assertThat(keyVersion.keyName()).isEqualTo(KEY_NAME);
            assertThat(keyVersion.keyVersion()).isEqualTo(KEY_VERSION);
        });
    }

    @Test
    void resolveAliasGetKeyCompletesExceptionally() {
        // given
        KmsException clientException = new KmsException("fail boom!");
        when(keyVaultClient.getKey(VAULT_NAME, KEY_NAME)).thenReturn(CompletableFuture.failedFuture(clientException));
        // when
        CompletionStage<WrappingKey> stage = azureKeyVaultKms.resolveAlias(KEY_NAME);
        // then
        assertThat(stage.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(KmsException.class).withMessage("failed to check existence of '" + KEY_NAME + "'")
                .havingCause().isSameAs(clientException);
    }

    @Test
    void resolveAliasGetKeyCompletesWithNull() {
        // given
        when(keyVaultClient.getKey(VAULT_NAME, KEY_NAME)).thenReturn(CompletableFuture.completedFuture(null));
        // when
        CompletionStage<WrappingKey> stage = azureKeyVaultKms.resolveAlias(KEY_NAME);
        // then
        assertThat(stage.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(KmsException.class).withMessage("get key returned null for: '" + KEY_NAME + "'");
    }

    @Test
    void resolveAliasKeyIsNotEnabled() {
        // given
        when(keyVaultClient.getKey(VAULT_NAME, KEY_NAME)).thenReturn(
                CompletableFuture.completedFuture(new GetKeyResponse(new JsonWebKey(KEY_ID, "RSA", REQUIRED_KEY_OPS), new KeyAttributes(false))));

        // when
        CompletionStage<WrappingKey> stage = azureKeyVaultKms.resolveAlias(KEY_NAME);
        // then
        assertThat(stage.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(KmsException.class).withMessage("key '" + KEY_NAME + "' is not enabled");
    }

    @Test
    void resolveAliasKeyIsNotFound() {
        // given
        when(keyVaultClient.getKey(VAULT_NAME, KEY_NAME)).thenReturn(
                CompletableFuture.failedFuture(new UnexpectedHttpStatusCodeException(404)));

        // when
        CompletionStage<WrappingKey> stage = azureKeyVaultKms.resolveAlias(KEY_NAME);
        // then
        assertThat(stage.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(UnknownAliasException.class).withMessage(KEY_NAME);
    }

    @Test
    void resolveAliasKeyIsNotFoundWithCompletionException() {
        // given
        when(keyVaultClient.getKey(VAULT_NAME, KEY_NAME)).thenReturn(
                CompletableFuture.failedFuture(new CompletionException(new UnexpectedHttpStatusCodeException(404))));

        // when
        CompletionStage<WrappingKey> stage = azureKeyVaultKms.resolveAlias(KEY_NAME);
        // then
        assertThat(stage.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(UnknownAliasException.class).withMessage(KEY_NAME);
    }

    static Stream<Arguments> resolveAliasKeyWithSufficientOperations() {
        return Stream.of(Arguments.argumentSet("wrap first", List.of("wrapKey", "unwrapKey")),
                Arguments.argumentSet("unwrap first", List.of("unwrapKey", "wrapKey")),
                Arguments.argumentSet("additional permissions after", List.of("wrapKey", "unwrapKey", "encrypt", "decrypt")),
                Arguments.argumentSet("additional permissions before", List.of("encrypt", "decrypt", "wrapKey", "unwrapKey")));
    }

    @MethodSource
    @ParameterizedTest
    void resolveAliasKeyWithSufficientOperations(List<String> keyOperations) {
        // given
        when(keyVaultClient.getKey(VAULT_NAME, KEY_NAME)).thenReturn(
                CompletableFuture.completedFuture(new GetKeyResponse(new JsonWebKey(KEY_ID, "RSA", keyOperations), new KeyAttributes(true))));

        // when
        CompletionStage<WrappingKey> stage = azureKeyVaultKms.resolveAlias(KEY_NAME);
        // then
        assertThat(stage.toCompletableFuture()).succeedsWithin(Duration.ZERO).satisfies(keyVersion -> {
            assertThat(keyVersion.supportedKeyType()).isEqualTo(SupportedKeyType.RSA);
            assertThat(keyVersion.keyName()).isEqualTo(KEY_NAME);
            assertThat(keyVersion.keyVersion()).isEqualTo(KEY_VERSION);
            assertThat(keyVersion.vaultName()).isEqualTo(VAULT_NAME);
        });
    }

    static Stream<Arguments> resolveAliasKeyWithInsufficientOperations() {
        return Stream.of(Arguments.argumentSet("wrap only", List.of("wrapKey")),
                Arguments.argumentSet("unwrap only", List.of("unwrapKey")),
                Arguments.argumentSet("empty", List.of()),
                Arguments.argumentSet("irrelevant ops", List.of("encrypt", "decrypt")));
    }

    @MethodSource
    @ParameterizedTest
    void resolveAliasKeyWithInsufficientOperations(List<String> keyOperations) {
        // given
        when(keyVaultClient.getKey(VAULT_NAME, KEY_NAME)).thenReturn(
                CompletableFuture.completedFuture(new GetKeyResponse(new JsonWebKey(KEY_ID, "RSA", keyOperations), new KeyAttributes(true))));

        // when
        CompletionStage<WrappingKey> stage = azureKeyVaultKms.resolveAlias(KEY_NAME);
        // then
        assertThat(stage.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(KmsException.class)
                .withMessage("key '" + KEY_NAME + "' key_ops " + keyOperations + " does not contain all of [wrapKey, unwrapKey]");
    }

    @CsvSource({ "EC", "EC-HSM", "bizarre-new-keytype-of-the-future" })
    @ParameterizedTest
    void resolveAliasKeyNotASupportedType(String keyType) {
        // given
        when(keyVaultClient.getKey(VAULT_NAME, KEY_NAME)).thenReturn(
                CompletableFuture.completedFuture(new GetKeyResponse(new JsonWebKey(KEY_ID, keyType, REQUIRED_KEY_OPS), new KeyAttributes(true))));
        // when
        CompletionStage<WrappingKey> stage = azureKeyVaultKms.resolveAlias(KEY_NAME);
        // then
        assertThat(stage.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(KmsException.class).withMessage("key '" + KEY_NAME + "' has an unsupported type " + keyType);
    }

    @Test
    void generateDekPairSuccess() {
        // given
        mockRandomGeneratorFillsArrayWith(DEK_BYTES);
        WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, SUPPORTED_KEY_TYPE, VAULT_NAME);
        when(keyVaultClient.wrap(eq(wrappingKey), any())).thenReturn(CompletableFuture.completedFuture(WRAPPED_DEK_BYTES));
        // when
        CompletionStage<DekPair<AzureKeyVaultEdek>> stage = azureKeyVaultKms.generateDekPair(wrappingKey);
        // then
        assertThat(stage.toCompletableFuture()).succeedsWithin(Duration.ZERO).satisfies(keyVersion -> {
            SecretKey dek = keyVersion.dek();
            assertThat(dek.getEncoded()).containsExactly(DEK_BYTES);
            assertThat(dek.getAlgorithm()).isEqualTo("aes");
            assertThat(dek.getFormat()).isEqualTo("RAW");
            AzureKeyVaultEdek edek = keyVersion.edek();
            assertThat(edek.edek()).containsExactly(WRAPPED_DEK_BYTES);
            assertThat(edek.keyName()).isEqualTo(KEY_NAME);
            assertThat(edek.keyVersion()).isEqualTo(KEY_VERSION);
            assertThat(edek.vaultName()).isEqualTo(VAULT_NAME);
            assertThat(edek.supportedKeyType()).isEqualTo(SUPPORTED_KEY_TYPE);
        });
    }

    @Test
    void generateDekPairWrapFailure() {
        // given
        mockRandomGeneratorFillsArrayWith(DEK_BYTES);
        SupportedKeyType supportedKeyType = SUPPORTED_KEY_TYPE;
        WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, supportedKeyType, VAULT_NAME);
        KmsException clientFailure = new KmsException("failed to wrap");
        when(keyVaultClient.wrap(eq(wrappingKey), any())).thenReturn(CompletableFuture.failedFuture(clientFailure));
        // when
        CompletionStage<DekPair<AzureKeyVaultEdek>> stage = azureKeyVaultKms.generateDekPair(wrappingKey);
        // then
        assertThat(stage.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(KmsException.class).withMessage(
                        "request to wrap DEK with key WrappingKey[keyName=" + KEY_NAME + ", keyVersion=" + KEY_VERSION + ", supportedKeyType=" + supportedKeyType.name()
                                + ", vaultName=" + VAULT_NAME + "] failed")
                .havingCause().isSameAs(clientFailure);
    }

    @Test
    void generateDekPairUnknownKey() {
        // given
        mockRandomGeneratorFillsArrayWith(DEK_BYTES);
        SupportedKeyType supportedKeyType = SUPPORTED_KEY_TYPE;
        WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, supportedKeyType, VAULT_NAME);
        UnexpectedHttpStatusCodeException clientFailure = new UnexpectedHttpStatusCodeException(404);
        when(keyVaultClient.wrap(eq(wrappingKey), any())).thenReturn(CompletableFuture.failedFuture(clientFailure));
        // when
        CompletionStage<DekPair<AzureKeyVaultEdek>> stage = azureKeyVaultKms.generateDekPair(wrappingKey);
        // then
        assertThat(stage.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(UnknownKeyException.class).withMessage(
                        "key '" + KEY_NAME + "' version '" + KEY_VERSION + "' not found while attempting to wrap");
    }

    @Test
    void generateDekPairUnknownKeyInCompletionException() {
        // given
        mockRandomGeneratorFillsArrayWith(DEK_BYTES);
        SupportedKeyType supportedKeyType = SUPPORTED_KEY_TYPE;
        WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, supportedKeyType, VAULT_NAME);
        UnexpectedHttpStatusCodeException clientFailure = new UnexpectedHttpStatusCodeException(404);
        when(keyVaultClient.wrap(eq(wrappingKey), any())).thenReturn(CompletableFuture.failedFuture(new CompletionException(clientFailure)));
        // when
        CompletionStage<DekPair<AzureKeyVaultEdek>> stage = azureKeyVaultKms.generateDekPair(wrappingKey);
        // then
        assertThat(stage.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(UnknownKeyException.class).withMessage(
                        "key '" + KEY_NAME + "' version '" + KEY_VERSION + "' not found while attempting to wrap");
    }

    @Test
    void decryptEdekSuccess() {
        // given
        WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, SUPPORTED_KEY_TYPE, VAULT_NAME);
        when(keyVaultClient.unwrap(eq(wrappingKey), any())).thenReturn(CompletableFuture.completedFuture(DEK_BYTES));
        // when
        CompletionStage<SecretKey> stage = azureKeyVaultKms.decryptEdek(
                new AzureKeyVaultEdek(KEY_NAME, KEY_VERSION, WRAPPED_DEK_BYTES, VAULT_NAME, SUPPORTED_KEY_TYPE));
        // then
        assertThat(stage.toCompletableFuture()).succeedsWithin(Duration.ZERO).satisfies(dek -> {
            assertThat(dek.getEncoded()).containsExactly(DEK_BYTES);
            assertThat(dek.getAlgorithm()).isEqualTo("aes");
            assertThat(dek.getFormat()).isEqualTo("RAW");
        });
        verify(keyVaultClient).unwrap(eq(wrappingKey), argThat(bytes -> Arrays.equals(WRAPPED_DEK_BYTES, bytes)));
    }

    @Test
    void decryptEdekUnwrapFailure() {
        // given
        KmsException failedToUnwrap = new KmsException("failed to unwrap");
        WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, SUPPORTED_KEY_TYPE, VAULT_NAME);
        when(keyVaultClient.unwrap(eq(wrappingKey), any())).thenReturn(CompletableFuture.failedFuture(failedToUnwrap));
        // when
        CompletionStage<SecretKey> stage = azureKeyVaultKms.decryptEdek(new AzureKeyVaultEdek(KEY_NAME, KEY_VERSION, WRAPPED_DEK_BYTES, VAULT_NAME, SUPPORTED_KEY_TYPE));
        // then
        assertThat(stage.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(KmsException.class).withMessage("failed to unwrap edek for key '" + KEY_NAME + "'")
                .havingCause().isSameAs(failedToUnwrap);
    }

    @Test
    void decryptEdekUnwrapNotFound() {
        // given
        UnexpectedHttpStatusCodeException failedToUnwrap = new UnexpectedHttpStatusCodeException(404);
        WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, SUPPORTED_KEY_TYPE, VAULT_NAME);
        when(keyVaultClient.unwrap(eq(wrappingKey), any())).thenReturn(CompletableFuture.failedFuture(failedToUnwrap));
        // when
        CompletionStage<SecretKey> stage = azureKeyVaultKms.decryptEdek(new AzureKeyVaultEdek(KEY_NAME, KEY_VERSION, WRAPPED_DEK_BYTES, VAULT_NAME, SUPPORTED_KEY_TYPE));
        // then
        assertThat(stage.toCompletableFuture()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(UnknownKeyException.class)
                .withMessage("key not found, key name: '" + KEY_NAME + "' version '" + KEY_VERSION + "'");
    }

    @Test
    void edekSerde() {
        assertThat(azureKeyVaultKms.edekSerde()).isNotNull();
    }

    private void mockRandomGeneratorFillsArrayWith(byte[] dekBytes) {
        Mockito.doAnswer(invocationOnMock -> {
            byte[] argument = invocationOnMock.getArgument(0);
            if (argument.length != 32) {
                throw new IllegalArgumentException("Invalid array length: " + argument.length);
            }
            System.arraycopy(dekBytes, 0, argument, 0, DEK_BYTES.length);
            return null;
        }).when(randomGenerator).nextBytes(any());
    }

}