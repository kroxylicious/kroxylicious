/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.net.ssl.SSLContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.aws.kms.model.EncryptRequest;
import io.kroxylicious.kms.provider.aws.kms.model.EncryptResponse;
import io.kroxylicious.kms.provider.fortanix.dsm.model.DecryptRequest;
import io.kroxylicious.kms.provider.fortanix.dsm.model.DecryptResponse;
import io.kroxylicious.kms.provider.fortanix.dsm.model.InfoRequest;
import io.kroxylicious.kms.provider.fortanix.dsm.model.InfoResponse;
import io.kroxylicious.kms.provider.fortanix.dsm.model.ErrorResponse;
import io.kroxylicious.kms.provider.fortanix.dsm.model.GenerateDataKeyResponse;
import io.kroxylicious.kms.provider.fortanix.dsm.model.SessionAuthResponse;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * An implementation of the KMS interface backed by <a href="https://www.fortanix.com/platform/data-security-manager">Fortanix DSM</a>.
 * <br/>
 */
public class FortanixDsmKms implements Kms<String, FortanixDsmKmsEdek> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String AES_KEY_ALGO = "AES";

    private static final TypeReference<SessionAuthResponse> SESSION_AUTH_RESPONSE = new TypeReference<>() {
    };

    private static final TypeReference<InfoResponse> INFO_KEY_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<List<EncryptResponse>> LIST_ENCRYPT_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<GenerateDataKeyResponse> GENERATE_DATA_KEY_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<DecryptResponse> DECRYPT_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<ErrorResponse> ERROR_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    static final String AUTHORIZATION_HEADER = "Authorization";
    public static final String ALIAS_PREFIX = "alias/";

    private final String apiKey;
    private final Duration timeout;
    private final HttpClient client;

    /**
     * The AWS KMS url.
     */
    private final URI awsUrl;
    private final KeyGenerator aes;
    private CompletableFuture<SessionAuthResponse> session;

    FortanixDsmKms(URI awsUrl, String apiKey, Duration timeout, SSLContext sslContext) {
        Objects.requireNonNull(awsUrl);
        Objects.requireNonNull(apiKey);
        this.awsUrl = awsUrl;
        this.apiKey = apiKey;
        this.timeout = timeout;
        client = createClient(sslContext);

        try {
            this.aes = KeyGenerator.getInstance(AES_KEY_ALGO);
            this.aes.init(256); // Required for Java 17 which defaults to a key size of 128.
        }
        catch (NoSuchAlgorithmException e) {
            // This should be impossible, because JCA guarantees that AES is available
            throw new KmsException(e);
        }

    }

    private HttpClient createClient(SSLContext sslContext) {
        HttpClient.Builder builder = HttpClient.newBuilder();
        if (sslContext != null) {
            builder.sslContext(sslContext);
        }
        return builder
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(timeout)
                .build();
    }

    /**
     * {@inheritDoc}
     * <br/>
     * @see <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_GenerateDataKey.html">https://docs.aws.amazon.com/kms/latest/APIReference/API_GenerateDataKey.html</a>
     */
    @NonNull
    @Override
    public CompletionStage<DekPair<FortanixDsmKmsEdek>> generateDekPair(@NonNull String kekRef) {
        try {
            var dek = DestroyableRawSecretKey.toDestroyableKey(this.aes.generateKey());

            // encrypt

            final EncryptRequest info = new EncryptRequest(kekRef, new EncryptRequest.Request(kekRef, "AES", dek.getEncoded(), null, null, 0, null ));
            return getSessionAuthResponse()
                    .thenApply(s -> createRequest(info, "/crypto/v1/keys/batch/encrypt", s))
                    .thenCompose(request -> sendAsync("", request, LIST_ENCRYPT_RESPONSE_TYPE_REF, u -> new UnknownAliasException(kekRef)))
                    .thenApply(x -> new DekPair<>(new FortanixDsmKmsEdek(kekRef, x.get(0).cipher()), dek));
        }
        catch (KmsException e) {
            return CompletableFuture.failedFuture(e);
        }

    }

    private static final String AES_WRAP_ALGO = "AES_256/GCM/NoPadding";

    private static Cipher aesGcm() {
        try {
            return Cipher.getInstance(AES_WRAP_ALGO);
        }
        catch (GeneralSecurityException e) {
            throw new KmsException(e);
        }
    }



    /**
     * {@inheritDoc}
     * <br/>
     * @see <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_Decrypt.html">https://docs.aws.amazon.com/kms/latest/APIReference/API_Decrypt.html</a>
     */
    @NonNull
    @Override
    public CompletionStage<SecretKey> decryptEdek(@NonNull FortanixDsmKmsEdek edek) {
        final DecryptRequest decryptRequest = new DecryptRequest(edek.kekRef(), edek.edek());
        var request = createRequest(decryptRequest, null, null);
        return sendAsync(edek.kekRef(), request, DECRYPT_RESPONSE_TYPE_REF, UnknownKeyException::new)
                .thenApply(response -> DestroyableRawSecretKey.takeOwnershipOf(response.plaintext(), AES_KEY_ALGO));
    }

    /**
     * {@inheritDoc}
     * <br/>
     * @see <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_DescribeKey.html">https://docs.aws.amazon.com/kms/latest/APIReference/API_DescribeKey.html</a>
     */
    @NonNull
    @Override
    public CompletableFuture<String> resolveAlias(@NonNull String alias) {
        final InfoRequest info = new InfoRequest(alias);
        return getSessionAuthResponse()
                .thenApply(s -> createRequest(info, "/crypto/v1/keys/info", s))
                .thenCompose(request -> sendAsync(alias, request, INFO_KEY_RESPONSE_TYPE_REF, u -> new UnknownAliasException(alias)))
                .thenApply(InfoResponse::kid);
    }

    private <T> CompletableFuture<T> sendAsync(@NonNull String key, HttpRequest request,
                                               TypeReference<T> valueTypeRef,
                                               Function<String, KmsException> exception) {
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(response -> checkResponseStatus(key, response, exception))
                .thenApply(HttpResponse::body)
                .thenApply(bytes -> decodeJson(valueTypeRef, bytes));
    }

    private static <T> T decodeJson(TypeReference<T> valueTypeRef, byte[] bytes) {
        try {
            return OBJECT_MAPPER.readValue(bytes, valueTypeRef);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @NonNull
    private static HttpResponse<byte[]> checkResponseStatus(@NonNull String key,
                                                            @NonNull HttpResponse<byte[]> response,
                                                            @NonNull Function<String, KmsException> notFound) {
        var statusCode = response.statusCode();
        // Our HTTP client is configured to follow redirects so 3xx responses are not expected here.
        var httpSuccess = statusCode >= 200 && statusCode < 300;
        if (!httpSuccess) {
            throw notFound.apply("Operation failed, request %s, HTTP status code %d".formatted(response.request().uri(), statusCode));
        }
        return response;
    }

    @NonNull
    @Override
    public Serde<FortanixDsmKmsEdek> edekSerde() {
        return FortanixDsmKmsEdekSerde.instance();
    }

    @NonNull
    private URI getEndpointUrl() {
        return awsUrl;
    }

    private HttpRequest createRequest(Object request, String path, SessionAuthResponse sessionAuth) {

        var body = getBody(request).getBytes(UTF_8);

        return HttpRequest.newBuilder()
                .uri(getEndpointUrl().resolve(path))
                .header(FortanixDsmKms.AUTHORIZATION_HEADER, sessionAuth.tokenType() + " " + sessionAuth.accessToken())
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
    }

    private CompletableFuture<SessionAuthResponse> getSessionAuthResponse() {
        if (this.session == null) {
            var sessionRequest = createSessionRequest();
            return sendAsync("session", sessionRequest, SESSION_AUTH_RESPONSE, KmsException::new)
                    .thenCompose(sar -> {
                        this.session = CompletableFuture.completedFuture(sar);
                        System.out.println("Got " + sar);
                        return this.session;
                    });
        }
        return this.session;
    }

    private HttpRequest createSessionRequest() {

        return HttpRequest.newBuilder()
                .uri(getEndpointUrl().resolve("/sys/v1/session/auth"))
                .header(FortanixDsmKms.AUTHORIZATION_HEADER, "Basic " + this.apiKey)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
    }


    private String getBody(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to create request body", e);
        }
    }

}
