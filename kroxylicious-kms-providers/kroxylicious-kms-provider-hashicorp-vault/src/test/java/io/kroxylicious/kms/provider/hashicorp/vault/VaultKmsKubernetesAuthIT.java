/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;

import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.Testcontainers;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

class VaultKmsKubernetesAuthIT {

    private TestVault testVault;
    private WireMockServer mockK8sApi;

    @BeforeEach
    void setUp() {
        assumeThat(DockerClientFactory.instance().isDockerAvailable()).withFailMessage("docker unavailable").isTrue();
        mockK8sApi = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        mockK8sApi.start();
        // Expose WireMock port to containers BEFORE starting the Vault container
        Testcontainers.exposeHostPorts(mockK8sApi.port());
        testVault = TestVault.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (testVault != null) {
            testVault.close();
        }
        if (mockK8sApi != null) {
            mockK8sApi.stop();
        }
    }

    @Test
    void testKubernetesAuthSucceeds(@TempDir Path tempDir) throws Exception {
        // 1. Generate RSA Key Pair for signing JWTs
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        KeyPair keyPair = kpg.generateKeyPair();

        // Build JWK (JSON Web Key) from the RSA public key - used for OIDC JWKS endpoint
        RsaJsonWebKey rsaJwk = (RsaJsonWebKey) JsonWebKey.Factory.newJwk(keyPair.getPublic());
        rsaJwk.setKeyId("k8s-test-key");
        rsaJwk.setUse("sig");
        String jwksJson = new JsonWebKeySet(rsaJwk).toJson();

        // 2. Generate a mock Kubernetes service account JWT
        JwtClaims claims = new JwtClaims();
        claims.setIssuer("kubernetes/serviceaccount");
        claims.setSubject("system:serviceaccount:default:test-sa");
        claims.setStringListClaim("aud", "vault");
        claims.setIssuedAtToNow();
        claims.setExpirationTimeMinutesInTheFuture(60);
        claims.setNotBeforeMinutesInThePast(1);
        claims.setClaim("kubernetes.io", java.util.Map.of(
                "namespace", "default",
                "serviceaccount", java.util.Map.of("name", "test-sa", "uid", "test-sa-uid-12345")));

        JsonWebSignature jws = new JsonWebSignature();
        jws.setPayload(claims.toJson());
        jws.setKey(keyPair.getPrivate());
        jws.setKeyIdHeaderValue("k8s-test-key");
        jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);
        String jwt = jws.getCompactSerialization();

        Path tokenFile = tempDir.resolve("token");
        Files.writeString(tokenFile, jwt);

        // 3. Stub the Kubernetes OIDC discovery endpoints so Vault can fetch public keys
        // Vault calls /.well-known/openid-configuration first to discover the JWKS URI
        String mockBaseUrl = "http://host.testcontainers.internal:" + mockK8sApi.port();
        String oidcDiscovery = """
                {
                  "issuer": "kubernetes/serviceaccount",
                  "jwks_uri": "%s/openid/v1/jwks",
                  "response_types_supported": ["id_token"],
                  "subject_types_supported": ["public"],
                  "id_token_signing_alg_values_supported": ["RS256"]
                }
                """.formatted(mockBaseUrl);
        mockK8sApi.stubFor(get(urlEqualTo("/.well-known/openid-configuration"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(oidcDiscovery)));

        // Stub the JWKS endpoint with our RSA public key
        mockK8sApi.stubFor(get(urlEqualTo("/openid/v1/jwks"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(jwksJson)));

        // 4. Stub the TokenReview API so Vault can validate the JWT identity
        String tokenReviewResponse = """
                {
                  "kind": "TokenReview",
                  "apiVersion": "authentication.k8s.io/v1",
                  "status": {
                    "authenticated": true,
                    "audiences": ["vault"],
                    "user": {
                      "username": "system:serviceaccount:default:test-sa",
                      "uid": "test-sa-uid-12345",
                      "groups": ["system:serviceaccounts", "system:serviceaccounts:default", "system:authenticated"],
                      "extra": {
                        "authentication.kubernetes.io/pod-namespace": ["default"]
                      }
                    }
                  }
                }
                """;
        mockK8sApi.stubFor(post(urlEqualTo("/apis/authentication.k8s.io/v1/tokenreviews"))
                .willReturn(aResponse()
                        .withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody(tokenReviewResponse)));

        // 5. Configure Vault Kubernetes Auth
        testVault.exec("vault", "auth", "enable", "kubernetes");
        testVault.exec("vault", "write", "auth/kubernetes/config",
                "kubernetes_host=" + mockBaseUrl,
                "token_reviewer_jwt=" + jwt,
                "disable_local_ca_jwt=true",
                "disable_iss_validation=true");

        testVault.exec("sh", "-c",
                "echo 'path \"transit/*\" { capabilities = [\"create\", \"read\", \"update\", \"delete\", \"list\"] }' > /tmp/test-policy.hcl");
        testVault.exec("vault", "policy", "write", "test-policy", "/tmp/test-policy.hcl");
        testVault.exec("vault", "write", "auth/kubernetes/role/test-role",
                "bound_service_account_names=test-sa",
                "bound_service_account_namespaces=default",
                "alias_name_source=serviceaccount_name",
                "token_policies=default,test-policy",
                "ttl=1h");

        // 6. Create KEK in vault
        String keyName = "mykey";
        testVault.createKek(keyName);

        // 7. Initialize KMS with Kubernetes Auth and verify we can resolve a key
        Config config = new Config(
                testVault.getEndpoint(),
                null,
                "test-role",
                tokenFile.toString(),
                "kubernetes",
                null);

        VaultKmsService service = new VaultKmsService();
        service.initialize(config);
        VaultKms vaultKms = service.buildKms();

        var resolved = vaultKms.resolveAlias(keyName);
        assertThat(resolved)
                .succeedsWithin(Duration.ofSeconds(10))
                .isEqualTo(keyName);
    }
}
