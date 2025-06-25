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
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import javax.crypto.SecretKey;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.fortanix.dsm.model.DecryptRequest;
import io.kroxylicious.kms.provider.fortanix.dsm.model.DecryptResponse;
import io.kroxylicious.kms.provider.fortanix.dsm.model.EncryptRequest;
import io.kroxylicious.kms.provider.fortanix.dsm.model.EncryptResponse;
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
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kms.provider.fortanix.dsm.model.Constants.AES;
import static io.kroxylicious.kms.provider.fortanix.dsm.model.Constants.EXPORT_KEY_OPS;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * An implementation of the KMS interface backed by <a href="https://www.fortanix.com/platform/data-security-manager">Fortanix DSM</a>.
 * It uses its <a href="https://support.fortanix.com/apidocs/">REST API</a>.
 */
public class FortanixDsmKms implements Kms<String, FortanixDsmKmsEdek> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final TypeReference<SecurityObjectResponse> SECURITY_OBJECT_RESPONSE_TYPE_REF = new TypeReference<SecurityObjectResponse>() {
    };

    private static final TypeReference<EncryptResponse> ENCRYPT_RESPONSE_TYPE_REF = new TypeReference<EncryptResponse>() {
    };
    private static final TypeReference<DecryptResponse> DECRYPT_RESPONSE_TYPE_REF = new TypeReference<DecryptResponse>() {
    };
    /**
     * HTTP Authentication header name.
     */
    public static final String AUTHORIZATION_HEADER = "Authorization";

    private final HttpClient client;

    /**
     * The Fortanix DSM API endpoint.
     */
    private final URI fortanixDsmUrl;
    private final SessionProvider sessionProvider;

    FortanixDsmKms(URI fortanixDsmUrl,
                   SessionProvider sessionProvider,
                   HttpClient client) {
        this.fortanixDsmUrl = Objects.requireNonNull(fortanixDsmUrl);
        this.sessionProvider = Objects.requireNonNull(sessionProvider);
        this.client = Objects.requireNonNull(client);
    }

    @VisibleForTesting
    HttpClient getHttpClient() {
        return this.client;
    }

    /**
     * {@inheritDoc}
     * <br/>
     * Fortanix DSM does not have a generate-data-key capability.  Instead, we use the /crypto/v1/keys endpoint to create an
     * AES key, export the key material then use the encrypt function to wrap the key using the KEK.
     *
     * @see <a href="https://support.fortanix.com/apidocs/generate-a-new-security-object">https://support.fortanix.com/apidocs/generate-a-new-security-object</a>
     * @see <a href="https://support.fortanix.com/apidocs/get-the-details-and-value-of-a-particular-exportable-security-object">https://support.fortanix.com/apidocs/get-the-details-and-value-of-a-particular-exportable-security-object</a>
     * @see <a href="https://support.fortanix.com/apidocs/encrypt-data-using-a-symmetric-or-asymmetric-key">https://support.fortanix.com/apidocs/encrypt-data-using-a-symmetric-or-asymmetric-key</a>
     */
    @Override
    public CompletionStage<DekPair<FortanixDsmKmsEdek>> generateDekPair(String kekRef) {
        var sessionFuture = getSessionFuture();
        var dekName = "dek-" + UUID.randomUUID();
        return sessionFuture
                .thenCompose(session -> createTransientKey(dekName, session))
                .thenCompose(transientKey -> exportTransientKey(dekName, transientKey, sessionFuture))
                .thenCompose(exportedKey -> wrapExportedTransientKey(kekRef, exportedKey, sessionFuture));
    }

    private CompletionStage<SecurityObjectResponse> createTransientKey(String dekName,
                                                                       Session session) {
        var transientKeySo = new SecurityObjectRequest(dekName, 256, AES, true, List.of(EXPORT_KEY_OPS), Map.of());
        var requestBuilder = createRequestBuilder(transientKeySo, "/crypto/v1/keys");

        return sendAsync(requestBuilder, SECURITY_OBJECT_RESPONSE_TYPE_REF, FortanixDsmKms::getStatusException, session);
    }

    private CompletionStage<SecurityObjectResponse> exportTransientKey(String dekName,
                                                                       SecurityObjectResponse transientKeyResponse,
                                                                       CompletionStage<Session> sessionFuture) {
        var requestBuilder = createRequestBuilder(new SecurityObjectDescriptor(null, null, transientKeyResponse.transientKey()),
                "/crypto/v1/keys/export");

        return sessionFuture
                .thenCompose(session -> sendAsync(requestBuilder, SECURITY_OBJECT_RESPONSE_TYPE_REF,
                        (uri, status) -> getStatusException(uri, status, () -> new UnknownKeyException(dekName)), session));
    }

    private CompletionStage<DekPair<FortanixDsmKmsEdek>> wrapExportedTransientKey(String kekRef,
                                                                                  SecurityObjectResponse exportedKey,
                                                                                  CompletionStage<Session> sessionFuture) {
        var secretKey = DestroyableRawSecretKey.takeOwnershipOf(exportedKey.value(), "AES");
        var encryptRequest = EncryptRequest.createWrapRequest(kekRef, exportedKey.value());

        var requestBuilder = createRequestBuilder(encryptRequest, "/crypto/v1/encrypt");
        return sessionFuture
                .thenCompose(session -> sendAsync(requestBuilder, ENCRYPT_RESPONSE_TYPE_REF,
                        (uri, statusCode) -> getStatusException(uri, statusCode, () -> new UnknownKeyException(kekRef)), session))
                .thenApply(encryptResponse -> new DekPair<>(new FortanixDsmKmsEdek(kekRef, encryptResponse.iv(), encryptResponse.cipher()), secretKey));
    }

    /**
     * {@inheritDoc}
     * <br/>
     * @see <a href="https://support.fortanix.com/apidocs/decrypt-data-using-a-symmetric-or-asymmetric-key">https://support.fortanix.com/apidocs/decrypt-data-using-a-symmetric-or-asymmetric-key</a>
    */
    @Override
    public CompletionStage<SecretKey> decryptEdek(FortanixDsmKmsEdek edek) {
        var sessionFuture = getSessionFuture();

        var decryptRequest = DecryptRequest.createUnwrapRequest(edek.kekRef(), edek.iv(), edek.edek());
        var requestBuilder = createRequestBuilder(decryptRequest, "/crypto/v1/decrypt");
        return sessionFuture
                .thenCompose(session -> sendAsync(requestBuilder, DECRYPT_RESPONSE_TYPE_REF,
                        (uri, status) -> getStatusException(uri, status, () -> new UnknownKeyException(edek.kekRef())), session))
                .thenApply(response -> DestroyableRawSecretKey.takeOwnershipOf(response.plain(), AES));
    }

    /**
     * {@inheritDoc}
     * <br/>
     * @see <a href="https://support.fortanix.com/apidocs/lookup-a-security-object">https://support.fortanix.com/apidocs/lookup-a-security-object</a>
     */
    @Override
    public CompletionStage<String> resolveAlias(String alias) {
        var sessionFuture = getSessionFuture();
        var descriptor = new SecurityObjectDescriptor(null, alias, null);
        var requestBuilder = createRequestBuilder(descriptor, "/crypto/v1/keys/info");
        return sessionFuture
                .thenCompose(session -> sendAsync(requestBuilder, SECURITY_OBJECT_RESPONSE_TYPE_REF,
                        (uri, status) -> getStatusException(uri, status, () -> new UnknownAliasException(alias)), session))
                .thenApply(SecurityObjectResponse::kid);
    }

    @Override
    public Serde<FortanixDsmKmsEdek> edekSerde() {
        return FortanixDsmKmsEdekSerde.instance();
    }

    private URI getEndpointUrl() {
        return fortanixDsmUrl;
    }

    private Builder createRequestBuilder(Object request, String path) {
        var body = getBody(request).getBytes(UTF_8);

        return HttpRequest.newBuilder()
                .uri(getEndpointUrl().resolve(path))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
    }

    private <T> CompletableFuture<T> sendAsync(Builder requestBuilder, TypeReference<T> valueTypeRef, BiFunction<URI, Integer, KmsException> exceptionSupplier,
                                               Session session) {

        var request = requestBuilder.header(AUTHORIZATION_HEADER, session.authorizationHeader())
                .build();

        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(response -> invalidateSessionIfNecessary(session, response))
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

    /**
     * If the response has indicates an authorization error it is likely the server has invalidated
     * the session (before its expiry).  If this case we invalidate the session.
     * @param session session
     * @param response response
     * @return response
     */
    private static HttpResponse<byte[]> invalidateSessionIfNecessary(Session session,
                                                                     HttpResponse<byte[]> response) {
        if (Set.of(401, 403).contains(response.statusCode())) {
            session.invalidate();
        }
        return response;
    }

    private static HttpResponse<byte[]> checkResponseStatus(HttpResponse<byte[]> response,
                                                            BiFunction<URI, Integer, KmsException> exceptionSupplier) {
        var statusCode = response.statusCode();
        var exception = exceptionSupplier.apply(response.request().uri(), statusCode);
        if (exception != null) {
            throw exception;
        }
        return response;
    }

    private static @Nullable KmsException getStatusException(URI uri,
                                                             int statusCode) {
        return getStatusException(uri, statusCode, null);
    }

    private static @Nullable KmsException getStatusException(URI uri,
                                                             int statusCode,
                                                             @Nullable Supplier<KmsException> notFoundSupplier) {
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

    private String getBody(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to create request body", e);
        }
    }

    private CompletionStage<Session> getSessionFuture() {
        return sessionProvider.getSession();
    }

}
