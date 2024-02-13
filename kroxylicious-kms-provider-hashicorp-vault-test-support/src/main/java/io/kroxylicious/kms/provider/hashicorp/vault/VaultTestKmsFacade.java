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
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.vault.VaultContainer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.UnknownAliasException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;

public class VaultTestKmsFacade extends AbstractVaultTestKmsFacade {
    private static final String HASHICORP_VAULT = "hashicorp/vault:1.15";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final HttpClient vaultClient = HttpClient.newHttpClient();

    @SuppressWarnings("rawtypes")
    private VaultContainer vaultContainer;

    @Override
    public boolean isAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    @Override
    @SuppressWarnings("resource")
    public void startVault() {
        vaultContainer = new VaultContainer<>(HASHICORP_VAULT).withVaultToken(VAULT_ROOT_TOKEN);
        vaultContainer.start();
    }

    @Override
    public void stopVault() {
        if (vaultContainer != null) {
            vaultContainer.close();
        }
    }

    @Override
    protected void enableTransit() {

        var engine = new EnableEngineRequest("transit");

        var body = getBody(engine);

        var request = createVaultPost("v1/sys/mounts/transit", BodyPublishers.ofString(body));

        sendRequestExpectingNoContentResponse(request);
    }

    @Override
    protected void createPolicy(String policyName, InputStream policyStream) {
        Objects.requireNonNull(policyName);
        Objects.requireNonNull(policyStream);
        var createPolicy = getBody(CreatePolicyRequest.fromInputStream(policyStream));
        var request = createVaultPost("v1/sys/policy/%s".formatted(encode(policyName, UTF_8)), BodyPublishers.ofString(createPolicy));

        sendRequestExpectingNoContentResponse(request);
    }

    @Override
    protected String createOrphanToken(String description, boolean noDefaultPolicy, Set<String> policies) {

        var token = new CreateTokenRequest(description, noDefaultPolicy, policies);

        String body = getBody(token);

        var request = createVaultPost("v1/auth/token/create-orphan", BodyPublishers.ofString(body));

        return sendRequest(request, new JsonBodyHandler<CreateTokenResponse>(new TypeReference<>() {
        })).auth().clientToken();
    }

    @Override
    @NonNull
    protected URI getVaultUrl() {
        return URI.create(vaultContainer.getHttpHostAddress());
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
            }
            else {
                create(alias);
            }
        }

        @Override
        public void rotateKek(String alias) {
            Objects.requireNonNull(alias);

            if (exists(alias)) {
                rotate(alias);
            }
            else {
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

            var request = createVaultPost("v1/transit/keys/%s".formatted(encode(keyId, UTF_8)), BodyPublishers.noBody());

            return sendRequest(request, statusHandler(keyId, request, new JsonBodyHandler<VaultResponse<VaultResponse.ReadKeyData>>(new TypeReference<>() {
            }))).data();
        }

        private VaultResponse.ReadKeyData read(String keyId) {
            var request = createVaultGet("v1/transit/keys/%s".formatted(encode(keyId, UTF_8)));

            return sendRequest(request, statusHandler(keyId, request, new JsonBodyHandler<VaultResponse<VaultResponse.ReadKeyData>>(new TypeReference<>() {
            }))).data();
        }

        private VaultResponse.ReadKeyData rotate(String keyId) {
            var request = createVaultPost("v1/transit/keys/%s/rotate".formatted(encode(keyId, UTF_8)), BodyPublishers.noBody());
            return sendRequest(request, statusHandler(keyId, request, new JsonBodyHandler<VaultResponse<VaultResponse.ReadKeyData>>(new TypeReference<>() {
            }))).data();
        }

    }

    private HttpRequest createVaultGet(String path) {
        return createVaultRequest()
                .uri(getVaultUrl().resolve(path))
                .GET()
                .build();
    }

    private HttpRequest createVaultPost(String path, BodyPublisher bodyPublisher) {
        URI resolve = getVaultUrl().resolve(path);
        return createVaultRequest()
                .uri(resolve)
                .POST(bodyPublisher).build();
    }

    private HttpRequest.Builder createVaultRequest() {
        return HttpRequest.newBuilder()
                .header("X-Vault-Token", VAULT_ROOT_TOKEN)
                .header("Accept", "application/json");
    }

    private <R> R sendRequest(HttpRequest request, HttpResponse.BodyHandler<Supplier<R>> responseBodyHandler) {
        try {
            return vaultClient.send(request, responseBodyHandler)
                    .body()
                    .get();
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

    private static <T> HttpResponse.BodyHandler<T> statusHandler(String keyId, HttpRequest request, HttpResponse.BodyHandler<T> handler) {
        return r -> {
            if (r.statusCode() == 404) {
                throw new UnknownAliasException(keyId);
            }
            else if (r.statusCode() != 200) {
                throw new IllegalStateException("unexpected response %s for request: %s".formatted(r.statusCode(), request.uri()));
            }
            return handler.apply(r);
        };
    }

}
