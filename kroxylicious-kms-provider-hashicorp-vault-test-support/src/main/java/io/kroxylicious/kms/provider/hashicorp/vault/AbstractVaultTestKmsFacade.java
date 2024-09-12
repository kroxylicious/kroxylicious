/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractVaultTestKmsFacade implements TestKmsFacade<Config, String, VaultEdek> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<VaultResponse<VaultResponse.ReadKeyData>> VAULT_RESPONSE_READ_KEY_DATA_TYPEREF = new TypeReference<>() {
    };
    private static final TypeReference<CreateTokenResponse> VAULT_RESPONSE_CREATE_TOKEN_RESPONSE_TYPEREF = new TypeReference<>() {
    };
    private static final String KEYS_PATH = "v1/transit/keys/%s";
    protected static final String VAULT_ROOT_TOKEN = "rootToken";
    private String kmsVaultToken;
    private final HttpClient vaultClient = HttpClient.newHttpClient();

    protected AbstractVaultTestKmsFacade() {
    }

    protected abstract void startVault();

    protected abstract void stopVault();

    @Override
    public final void start() {

        startVault();

        // enable transit engine
        enableTransit();

        // create policy
        var policyName = "kroxylicious_encryption_filter_policy";
        try (var policyStream = AbstractVaultTestKmsFacade.class.getResourceAsStream("kroxylicious_encryption_filter_policy.hcl")) {
            createPolicy(policyName, policyStream);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create policy", e);
        }
        // create least-privilege vault token that KMS will use. The decision to use orphan tokens
        // (https://developer.hashicorp.com/vault/docs/concepts/tokens#token-hierarchies-and-orphan-tokens)
        // but no functional impact. A child token would work too.
        kmsVaultToken = createOrphanToken("kroxylicious_encryption_filter", true, Set.of(policyName));
    }

    protected void enableTransit() {
        var engine = new EnableEngineRequest("transit");
        var body = getBody(engine);
        var request = createVaultPost("v1/sys/mounts/transit", HttpRequest.BodyPublishers.ofString(body));

        sendRequestExpectingNoContentResponse(request);
    }

    protected void createPolicy(String policyName, InputStream policyStream) {
        Objects.requireNonNull(policyName);
        Objects.requireNonNull(policyStream);
        var createPolicy = getBody(CreatePolicyRequest.fromInputStream(policyStream));
        var request = createVaultPost("v1/sys/policy/%s".formatted(encode(policyName, UTF_8)), HttpRequest.BodyPublishers.ofString(createPolicy));

        sendRequestExpectingNoContentResponse(request);
    }

    protected String createOrphanToken(String description, boolean noDefaultPolicy, Set<String> policies) {
        var token = new CreateTokenRequest(description, noDefaultPolicy, policies);
        String body = getBody(token);
        var request = createVaultPost("v1/auth/token/create-orphan", HttpRequest.BodyPublishers.ofString(body));

        return sendRequest("dummy", request, VAULT_RESPONSE_CREATE_TOKEN_RESPONSE_TYPEREF).auth().clientToken();
    }

    @NonNull
    protected abstract URI getVaultUrl();

    @Override
    public final Config getKmsServiceConfig() {
        return new Config(getVaultTransitEngineUrl(), new InlinePassword(kmsVaultToken), null);
    }

    @Override
    public final Class<VaultKmsService> getKmsServiceClass() {
        return VaultKmsService.class;
    }

    @Override
    public final void stop() {
        stopVault();
    }

    @NonNull
    protected final URI getVaultTransitEngineUrl() {
        return getVaultUrl().resolve("v1/transit/");
    }

    @Override
    public TestKekManager getTestKekManager() {
        return new VaultTestKekManager();
    }

    class VaultTestKekManager implements TestKekManager {
        @Override
        public void generateKek(String alias) {
            Objects.requireNonNull(alias);

            if (exists(alias)) {
                throw new AlreadyExistsException(alias);
            } else {
                create(alias);
            }
        }

        @Override
        public void deleteKek(String alias) {
            if (exists(alias)) {
                delete(alias);
            } else {
                throw new UnknownAliasException(alias);
            }

        }

        @Override
        public void rotateKek(String alias) {
            Objects.requireNonNull(alias);

            if (exists(alias)) {
                rotate(alias);
            } else {
                throw new UnknownAliasException(alias);
            }
        }

        @Override
        public boolean exists(String alias) {
            try {
                read(alias);
                return true;
            }
            catch (UnknownAliasException uae) {
                return false;
            }
        }

        private VaultResponse.ReadKeyData create(String keyId) {
            var request = createVaultPost(KEYS_PATH.formatted(encode(keyId, UTF_8)), HttpRequest.BodyPublishers.noBody());
            return sendRequest(keyId, request, VAULT_RESPONSE_READ_KEY_DATA_TYPEREF).data();
        }

        private void delete(String keyId) {
            var update = createVaultPost(
                    (KEYS_PATH + "/config").formatted(encode(keyId, UTF_8)),
                    HttpRequest.BodyPublishers.ofString(getBody(new UpdateKeyConfigRequest(true)))
            );
            sendRequest(keyId, update, VAULT_RESPONSE_READ_KEY_DATA_TYPEREF);

            var delete = createVaultDelete(KEYS_PATH.formatted(encode(keyId, UTF_8)));
            sendRequestExpectingNoContentResponse(delete);
        }

        private VaultResponse.ReadKeyData read(String keyId) {
            var request = createVaultGet(KEYS_PATH.formatted(encode(keyId, UTF_8)));
            return sendRequest(keyId, request, VAULT_RESPONSE_READ_KEY_DATA_TYPEREF).data();
        }

        private VaultResponse.ReadKeyData rotate(String keyId) {
            var request = createVaultPost((KEYS_PATH + "/rotate").formatted(encode(keyId, UTF_8)), HttpRequest.BodyPublishers.noBody());
            return sendRequest(keyId, request, VAULT_RESPONSE_READ_KEY_DATA_TYPEREF).data();
        }

    }

    private HttpRequest createVaultGet(String path) {
        return createVaultRequest()
                                   .uri(getVaultUrl().resolve(path))
                                   .GET()
                                   .build();
    }

    private HttpRequest createVaultDelete(String path) {
        return createVaultRequest()
                                   .uri(getVaultUrl().resolve(path))
                                   .DELETE()
                                   .build();
    }

    private HttpRequest createVaultPost(String path, HttpRequest.BodyPublisher bodyPublisher) {
        URI resolve = getVaultUrl().resolve(path);
        return createVaultRequest()
                                   .uri(resolve)
                                   .POST(bodyPublisher)
                                   .build();
    }

    private HttpRequest.Builder createVaultRequest() {
        return HttpRequest.newBuilder()
                          .header("X-Vault-Token", VAULT_ROOT_TOKEN)
                          .header("Accept", "application/json");
    }

    private <R> R sendRequest(String key, HttpRequest request, TypeReference<R> valueTypeRef) {
        try {
            HttpResponse<byte[]> response = vaultClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() == 404) {
                throw new UnknownAliasException(key);
            } else if (response.statusCode() != 200) {
                throw new IllegalStateException("unexpected response %s for request: %s".formatted(response.statusCode(), request.uri()));
            }
            byte[] body = response
                                  .body();
            return decodeJson(valueTypeRef, body);
        }
        catch (IOException e) {
            if (e.getCause() instanceof KmsException ke) {
                throw ke;
            }
            throw new UncheckedIOException("Request to %s failed".formatted(request), e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during REST API call : %s".formatted(request.uri()), e);
        }
    }

    private void sendRequestExpectingNoContentResponse(HttpRequest request) {
        try {
            var response = vaultClient.send(request, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() != 204) {
                throw new IllegalStateException("Unexpected response : %d to request %s".formatted(response.statusCode(), request.uri()));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Request to %s failed".formatted(request), e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private String getBody(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to create request body", e);
        }
    }

    private static <T> T decodeJson(TypeReference<T> valueTypeRef, byte[] bytes) {
        try {
            return OBJECT_MAPPER.readValue(bytes, valueTypeRef);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
