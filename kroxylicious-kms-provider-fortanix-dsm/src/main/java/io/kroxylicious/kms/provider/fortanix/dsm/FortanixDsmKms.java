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
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import javax.crypto.SecretKey;
import javax.net.ssl.SSLContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.fortanix.dsm.model.DecryptRequest;
import io.kroxylicious.kms.provider.fortanix.dsm.model.DecryptResponse;
import io.kroxylicious.kms.provider.fortanix.dsm.model.EncryptRequest;
import io.kroxylicious.kms.provider.fortanix.dsm.model.EncryptResponse;
import io.kroxylicious.kms.provider.fortanix.dsm.model.ResponseBodyContainer;
import io.kroxylicious.kms.provider.fortanix.dsm.model.SecurityObjectDescriptor;
import io.kroxylicious.kms.provider.fortanix.dsm.model.SecurityObjectRequest;
import io.kroxylicious.kms.provider.fortanix.dsm.model.SecurityObjectResponse;
import io.kroxylicious.kms.provider.fortanix.dsm.session.Session;
import io.kroxylicious.kms.provider.fortanix.dsm.session.SessionProvider;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * An implementation of the KMS interface backed by <a href="https://www.fortanix.com/platform/data-security-manager">Fortanix DSM</a> implemented using
 * the <a href="https://support.fortanix.com/apidocs/">REST API</a>.
 */
public class FortanixDsmKms implements Kms<String, FortanixDsmKmsEdek> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String AES_KEY_ALGO = "AES";

    private static final TypeReference<SecurityObjectResponse> SECURITY_OBJECT_RESPONSE_TYPE_REF = new TypeReference<>() {
    };

    private static final TypeReference<List<EncryptResponse>> LIST_ENCRYPT_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<List<DecryptResponse>> LIST_DECRYPT_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    public static final String AUTHORIZATION_HEADER = "Authorization";

    private final Duration timeout;
    private final HttpClient client;

    /**
     * The Fortanix DSM API endpoint.
     */
    private final URI fortanixDsmUrl;
    private final SessionProvider sessionProvider;

    FortanixDsmKms(URI fortanixDsmUrl, Duration timeout, SSLContext sslContext, SessionProvider sessionProvider) {
        this.fortanixDsmUrl = Objects.requireNonNull(fortanixDsmUrl);
        this.sessionProvider = Objects.requireNonNull(sessionProvider);
        this.timeout = timeout;
        this.client = createClient(sslContext);
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
     * Fortanix DSM does not have a generate-data-key capability.  Instead, we use the /crypto/v1/keys endpoint to create an
     * AES key, export the key material then use the encrypt function to wrap the key using the KEK.
     *
     * @see <a href="https://support.fortanix.com/apidocs/generate-a-new-security-object">https://support.fortanix.com/apidocs/generate-a-new-security-object</a>
     * @see <a href="https://support.fortanix.com/apidocs/get-the-details-and-value-of-a-particular-exportable-security-object">https://support.fortanix.com/apidocs/get-the-details-and-value-of-a-particular-exportable-security-object</a>
     * @see <a href="https://support.fortanix.com/apidocs/batch-encrypt-with-one-or-more-keys">https://support.fortanix.com/apidocs/batch-encrypt-with-one-or-more-keys</a>
     */
    @NonNull
    @Override
    public CompletionStage<DekPair<FortanixDsmKmsEdek>> generateDekPair(@NonNull String kekRef) {
        try {

            var dekName = "dek-" + UUID.randomUUID();
            var transientKeyRequest = new SecurityObjectRequest(dekName, 256, "AES", true, List.of("EXPORT"), Map.of());

            var sessionFuture = getSessionAuthResponse();

            return sessionFuture
                    .thenApply(s -> createRequest(transientKeyRequest, "/crypto/v1/keys", s))
                    .thenCompose(request -> sendAsync(request, SECURITY_OBJECT_RESPONSE_TYPE_REF, FortanixDsmKms::getStatusException))

                    .thenCombine(sessionFuture,
                            (transientKeyResponse, s) -> createRequest(new SecurityObjectDescriptor(null, null, transientKeyResponse.transientKey()),
                                    "/crypto/v1/keys/export",
                                    s))
                    .thenCompose(request -> sendAsync(request, SECURITY_OBJECT_RESPONSE_TYPE_REF,
                            (uri, status) -> getStatusException(uri, status, () -> new UnknownKeyException(dekName))))

                    .thenCompose(export -> wrapExportedTransientKey(kekRef, export, sessionFuture));
        }
        catch (KmsException e) {
            return CompletableFuture.failedFuture(e);
        }

    }

    private CompletionStage<DekPair<FortanixDsmKmsEdek>> wrapExportedTransientKey(@NonNull String kekRef, @NonNull SecurityObjectResponse exportedKey,
                                                                                  @NonNull CompletionStage<Session> sessionFuture) {
        var secretKey = DestroyableRawSecretKey.takeOwnershipOf(exportedKey.value(), "AES");
        var batchEncryptRequests = List.of(EncryptRequest.createWrapRequest(kekRef, exportedKey.value()));

        return sessionFuture
                .thenApply(s -> createRequest(batchEncryptRequests, "/crypto/v1/keys/batch/encrypt", s))
                .thenCompose(request -> sendAsync(request, LIST_ENCRYPT_RESPONSE_TYPE_REF,
                        (uri, statusCode) -> getStatusException(uri, statusCode, () -> new UnknownKeyException(kekRef))))
                .thenApply(this::singletonListToValue)
                .thenApply(encryptResponse -> validateResponseBody(encryptResponse, kekRef))
                .thenApply(encryptResponse -> new DekPair<>(new FortanixDsmKmsEdek(kekRef, encryptResponse.cipher(), encryptResponse.iv()), secretKey));
    }

    /**
     * {@inheritDoc}
     * <br/>
     * @see <a href="https://support.fortanix.com/apidocs/batch-decrypt-with-one-or-more-keys">https://support.fortanix.com/apidocs/batch-decrypt-with-one-or-more-keys</a>
    */
    @NonNull
    @Override
    public CompletionStage<SecretKey> decryptEdek(@NonNull FortanixDsmKmsEdek edek) {
        var sessionFuture = getSessionAuthResponse();

        final var batchEncryptRequests = List.of(DecryptRequest.createUnwrapRequest(edek.kekRef(), edek.iv(), edek.edek()));
        return sessionFuture
                .thenApply(s -> createRequest(batchEncryptRequests, "/crypto/v1/keys/batch/decrypt", s))
                .thenCompose(request -> sendAsync(request, LIST_DECRYPT_RESPONSE_TYPE_REF,
                        (uri, status) -> getStatusException(uri, status, () -> new UnknownKeyException(edek.kekRef()))))
                .thenApply(this::singletonListToValue)
                .thenApply(encryptResponse -> validateResponseBody(encryptResponse, edek.kekRef()))
                .thenApply(response -> DestroyableRawSecretKey.takeOwnershipOf(response.plain(), AES_KEY_ALGO));
    }

    /**
     * {@inheritDoc}
     * <br/>
     * @see <a href="https://support.fortanix.com/apidocs/lookup-a-security-object">https://support.fortanix.com/apidocs/lookup-a-security-object</a>
     */
    @NonNull
    @Override
    public CompletionStage<String> resolveAlias(@NonNull String alias) {
        var descriptor = new SecurityObjectDescriptor(null, alias, null);
        return getSessionAuthResponse()
                .thenApply(s -> createRequest(descriptor, "/crypto/v1/keys/info", s))
                .thenCompose(request -> sendAsync(request, SECURITY_OBJECT_RESPONSE_TYPE_REF,
                        (uri, status) -> getStatusException(uri, status, () -> new UnknownAliasException(alias))))
                .thenApply(SecurityObjectResponse::kid);
    }

    private <T> CompletableFuture<T> sendAsync(HttpRequest request, TypeReference<T> valueTypeRef, BiFunction<URI, Integer, KmsException> exceptionSupplier) {
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(response -> checkResponseStatus(response, exceptionSupplier))
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
    private static HttpResponse<byte[]> checkResponseStatus(@NonNull HttpResponse<byte[]> response, BiFunction<URI, Integer, KmsException> exceptionSupplier) {
        var statusCode = response.statusCode();
        var exception = exceptionSupplier.apply(response.request().uri(), statusCode);
        if (exception != null) {
            throw exception;
        }
        return response;
    }

    private static @Nullable KmsException getStatusException(@NonNull URI uri, int statusCode) {
        return getStatusException(uri, statusCode, null);
    }

    private static @Nullable KmsException getStatusException(@NonNull URI uri, int statusCode, @Nullable Supplier<KmsException> notFoundSupplier) {
        // Our HTTP client is configured to follow redirects so 3xx responses are not expected here.
        KmsException cause = null;
        if (!(statusCode >= 200 && statusCode < 300)) {
            cause = new KmsException("Operation failed, request %s, HTTP status code %d".formatted(uri, statusCode));
        }

        if (notFoundSupplier != null && statusCode == 404) {
            var notFound = notFoundSupplier.get();
            if (notFound != null) {
                notFound.initCause(cause);
                return notFound;
            }
        }
        return cause;
    }

    @NonNull
    @Override
    public Serde<FortanixDsmKmsEdek> edekSerde() {
        return FortanixDsmKmsEdekSerde.instance();
    }

    @NonNull
    private URI getEndpointUrl() {
        return fortanixDsmUrl;
    }

    private HttpRequest createRequest(Object request, String path, Session sessionAuth) {

        var body = getBody(request).getBytes(UTF_8);

        return HttpRequest.newBuilder()
                .uri(getEndpointUrl().resolve(path))
                .header(FortanixDsmKms.AUTHORIZATION_HEADER, sessionAuth.authorizationHeader())
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
    }

    private CompletionStage<Session> getSessionAuthResponse() {
        return sessionProvider.getSession();
    }

    private String getBody(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to create request body", e);
        }
    }

    private <R> R singletonListToValue(List<R> encryptResponses) {
        var num = encryptResponses.size();
        if (num != 1) {
            throw new KmsException("expecting list to contained exactly one response, but it contains:" + num);
        }
        return encryptResponses.get(0);
    }

    private <R> R validateResponseBody(ResponseBodyContainer<R> container, String kekRef) {
        int status = container.status();
        if (status != 200) {
            if (status == 404) {
                throw new UnknownKeyException(kekRef);
            }
            throw new KmsException("encrypt response has unexpected status code : " + status);
        }
        return container.body();
    }

}
