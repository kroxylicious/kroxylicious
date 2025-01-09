/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import java.net.URI;
import java.util.Optional;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;

import io.kroxylicious.kms.provider.fortanix.dsm.config.Config;
import io.kroxylicious.kms.provider.fortanix.dsm.model.SecurityObjectResponse;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKekManager.AlreadyExistsException;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FortanixDsmKmsTestKmsFacadeTest {

    private static WireMockServer server;
    private static final String SESSION_AUTH_RESPONSE = """
            {"token_type":"Bearer",
             "expires_in":60000,
             "access_token":"tok",
             "entity_id":"ent",
             "allowed_mfa_methods":[]}
            """;

    private static final String CREATE_KEK_RESPONSE = """
            {"kid": "kid"}
             """;
    private FortanixDsmKmsTestKmsFacade facade;
    private TestKekManager manager;

    @BeforeAll
    public static void initMockRegistry() {
        server = new WireMockServer(wireMockConfig().dynamicPort());
        server.start();
    }

    @AfterAll
    public static void shutdownMockRegistry() {
        server.shutdown();
    }

    @BeforeEach
    void setUp() {
        server.stubFor(
                post(urlEqualTo("/sys/v1/session/auth"))
                        .willReturn(aResponse().withBody(SESSION_AUTH_RESPONSE)));
        facade = new FortanixDsmKmsTestKmsFacade(Optional.of(URI.create(server.baseUrl())), Optional.of("unused"), Optional.of("unused")) {
            @Override
            protected void deleteTestKeks() {
                // we don't need to delete test resources as we are testing against a mock.
            }
        };
        facade.start();
        manager = facade.getTestKekManager();
    }

    @AfterEach
    void afterEach() {
        try {
            facade.stop();
            server.resetAll();
        }
        finally {
            Optional.ofNullable(facade).ifPresent(FortanixDsmKmsTestKmsFacade::close);
        }
    }

    @Test
    void generateKek() {
        var alias = "alias";

        server.stubFor(post(urlEqualTo("/crypto/v1/keys"))
                .withRequestBody(matchingJsonPath("$.name", equalTo(alias)))
                .willReturn(aResponse().withBody(CREATE_KEK_RESPONSE)));

        manager.generateKek(alias);

        server.verify(1, postRequestedFor(urlEqualTo("/crypto/v1/keys")));
    }

    @Test
    void generateKekFailsDueToConflict() {
        var alias = "alias";

        server.stubFor(post(urlEqualTo("/crypto/v1/keys"))
                .willReturn(aResponse().withStatus(409)));

        assertThatThrownBy(() -> manager.generateKek(alias))
                .isInstanceOf(AlreadyExistsException.class);
    }

    @Test
    void readKek() {
        var alias = "alias";

        server.stubFor(post(urlEqualTo("/crypto/v1/keys/info"))
                .withRequestBody(matchingJsonPath("$.name", equalTo(alias)))
                .willReturn(aResponse().withBody("""
                        {"kid": "thekid"}
                         """)));

        var kek = manager.read(alias);

        assertThat(kek)
                .asInstanceOf(InstanceOfAssertFactories.type(SecurityObjectResponse.class))
                .extracting(SecurityObjectResponse::kid)
                .isEqualTo("thekid");

        server.verify(1, postRequestedFor(urlEqualTo("/crypto/v1/keys/info")));
    }

    @Test
    void readKekFailsDueToNotFound() {
        var alias = "alias";

        server.stubFor(post(urlEqualTo("/crypto/v1/keys/info"))
                .withRequestBody(matchingJsonPath("$.name", equalTo(alias)))
                .willReturn(aResponse().withStatus(404)));

        assertThatThrownBy(() -> manager.read(alias))
                .isInstanceOf(UnknownAliasException.class);
    }

    @Test
    void readKekDueToInternalServerError() {
        var alias = "alias";

        server.stubFor(post(urlEqualTo("/crypto/v1/keys/info"))
                .willReturn(aResponse().withStatus(500)));

        assertThatThrownBy(() -> manager.read(alias))
                .isInstanceOf(KmsException.class);
    }

    @Test
    void readKekDueToUnexpectedResponse() {
        var alias = "alias";

        server.stubFor(post(urlEqualTo("/crypto/v1/keys/info"))
                .willReturn(aResponse().withBody("[]")));

        assertThatThrownBy(() -> manager.read(alias))
                .isInstanceOf(KmsException.class);
    }

    @Test
    void deleteKek() {
        var alias = "alias";

        server.stubFor(post(urlEqualTo("/crypto/v1/keys/info"))
                .withRequestBody(matchingJsonPath("$.name", equalTo(alias)))
                .willReturn(aResponse().withBody("""
                        {"kid": "thekid"}
                         """)));

        server.stubFor(delete(urlEqualTo("/crypto/v1/keys/thekid"))
                .willReturn(aResponse().withStatus(204)));

        manager.deleteKek(alias);

        server.verify(1, deleteRequestedFor(urlEqualTo("/crypto/v1/keys/thekid")));
    }

    @Test
    void deleteKekFailsDueToAliasNotFound() {
        var alias = "alias";

        server.stubFor(post(urlEqualTo("/crypto/v1/keys/info"))
                .withRequestBody(matchingJsonPath("$.name", equalTo(alias)))
                .willReturn(aResponse().withBody("""
                        {"kid": "thekid"}
                         """)));

        server.stubFor(delete(urlEqualTo("/crypto/v1/keys/thekid"))
                .willReturn(aResponse().withStatus(404)));

        assertThatThrownBy(() -> manager.deleteKek(alias))
                .isInstanceOf(UnknownKeyException.class);
    }

    @Test
    void deleteKekFailsDueToKeyNotFound() {
        var alias = "alias";

        server.stubFor(post(urlEqualTo("/crypto/v1/keys/info"))
                .withRequestBody(matchingJsonPath("$.name", equalTo(alias)))
                .willReturn(aResponse().withStatus(404)));

        assertThatThrownBy(() -> manager.deleteKek(alias))
                .isInstanceOf(UnknownAliasException.class);
    }

    @Test
    void rotateKek() {
        var alias = "alias";

        server.stubFor(post(urlEqualTo("/crypto/v1/keys/rekey"))
                .withRequestBody(matchingJsonPath("$.name", equalTo(alias)))
                .willReturn(aResponse().withBody(CREATE_KEK_RESPONSE)));

        manager.rotateKek(alias);

        server.verify(1, postRequestedFor(urlEqualTo("/crypto/v1/keys/rekey")));
    }

    @Test
    void rotateKekFailsDueToNotFound() {
        var alias = "alias";

        server.stubFor(post(urlEqualTo("/crypto/v1/keys/rekey"))
                .withRequestBody(matchingJsonPath("$.name", equalTo(alias)))
                .willReturn(aResponse().withStatus(404)));

        assertThatThrownBy(() -> manager.deleteKek(alias))
                .isInstanceOf(UnknownAliasException.class);
    }

    @Test
    void isAvailable() {
        assertThat(facade.isAvailable()).isTrue();
    }

    @Test
    void classAndConfig() {
        assertThat(facade.getKmsServiceClass()).isEqualTo(FortanixDsmKmsService.class);
        assertThat(facade.getKmsServiceConfig()).isInstanceOf(Config.class);
    }

}
