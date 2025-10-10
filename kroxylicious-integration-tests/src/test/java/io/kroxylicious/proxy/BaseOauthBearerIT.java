/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.RestoreSystemProperties;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.NonNull;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
@EnabledIf(value = "isDockerAvailable", disabledReason = "docker unavailable")
@RestoreSystemProperties
public class BaseOauthBearerIT {
    private static final int OAUTH_SERVER_PORT = 28089;

    // This token is expired
    protected static final String LONG_SINCE_EXPIRED_TOKEN = "eyJraWQiOiJkZWZhdWx0IiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ."
            + "eyJzdWIiOiJjbGllbnRJZElnbm9yZSIsImF1ZCI6ImRlZmF1bHQiLCJuYmYiOjE3MjA3MjIyOTAsImF6cCI6ImNsaWVudElkSWdub3JlIiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDoyODA4OS9kZWZhdWx0IiwiZXhwIjoxNzIwNzI1ODkwLCJpYXQiOjE3MjA3MjIyOTAsImp0aSI6IjE0MGZjMmFhLWRjZmQtNDE1Mi05MWJmLWQyMDZiM2M1MzAxZiIsInRpZCI6ImRlZmF1bHQifQ."
            + "I4LUI4Lp6XwUzD-UN2LDRfiNPCvhHDQJVH_LCE4gkuY5UxrRMJ8H3C9408zmjGPRChWmKU4aRIy8rtSPbfRmDLenoM91dr1mDy8B01TZohEOACnAxBSvsN73-cNUUvaRzZmMeUkGbmgEtGhqZ2d3MELe7bm0fEyuNRyM9fv-AahGm551hchMe3bzeYjbcBKuatKYoiHuPTX_HuNF4AAwI_vz_lYzHliKDmPRJwpsaMUaCtsfUKbSzpRPe6X2FWWkkgOLtCD-W14sp3r8z2KrHryH_ILz2MtPvIlvmkJE8U0CRaVyQ-6L2sL-iUdXoUGTHmV384X2R-cy5gKFhN3Ibg"; // notsecret

    // This token has no kid field, which will cause BrokerJwtValidator to reject it.
    public static final String NO_KEYID_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9."
            + "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTc2MDQ1NzMzNSwiZXhwIjoxNzYwNDYwOTM1fQ."
            + "xpOs85uRq4GuKBc2RnJinPsbFbGSIExuCZ8c2oYzWq4"; // notsecret

    protected static final String JWKS_ENDPOINT_URL = "http://localhost:" + OAUTH_SERVER_PORT + "/default/jwks";
    private static final URI OAUTH_ENDPOINT_URL = URI.create(JWKS_ENDPOINT_URL).resolve("/");
    protected static final URI TOKEN_ENDPOINT_URL = OAUTH_ENDPOINT_URL.resolve("default/token");
    protected static final String EXPECTED_AUDIENCE = "default";
    protected static final String ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG = "org.apache.kafka.sasl.oauthbearer.allowed.urls";
    protected static final String CLIENT_ID = "clientId-" + UUID.randomUUID();
    protected static final String CLIENT_SECRET = "clientSecret";

    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("ghcr.io/navikt/mock-oauth2-server:3.0.0");
    private static OauthServerContainer oauthServer;

    @BeforeAll
    static void beforeAll() {
        // Kafka 4.0 requires that the org.apache.kafka.sasl.oauthbearer.allowed.urls sys property is set in order to use Oauth Bearer.
        // The Kafka Broker and Proxy requires that JWKS_ENDPOINT_URL is in the allow list.
        // The Kafka Client requires that TOKEN_ENDPOINT_URL is in the allow list.
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, JWKS_ENDPOINT_URL + "," + TOKEN_ENDPOINT_URL);

        oauthServer = new OauthServerContainer(BaseOauthBearerIT.DOCKER_IMAGE_NAME);
        oauthServer.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*started server on address.*"));
        oauthServer.addFixedExposedPort(OAUTH_SERVER_PORT, OAUTH_SERVER_PORT);
        oauthServer.withEnv("SERVER_PORT", OAUTH_SERVER_PORT + "");
        oauthServer.withEnv("LOG_LEVEL", "DEBUG"); // required to for the startup message to be logged.
        oauthServer.start();
    }

    @AfterAll
    static void afterAll() {
        if (oauthServer != null) {
            oauthServer.close();
        }
    }

    static boolean isDockerAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    protected Map<String, Object> getClientConfig(URI tokenEndpointUrl) {
        var oauthClientConfig = new HashMap<String, Object>();
        oauthClientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        oauthClientConfig.put(SaslConfigs.SASL_MECHANISM, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM);
        oauthClientConfig.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl.toString());
        oauthClientConfig.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, OAuthBearerLoginCallbackHandler.class.getCanonicalName());
        oauthClientConfig.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, CLIENT_ID);
        oauthClientConfig.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, CLIENT_SECRET);
        oauthClientConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
                "%s required;".formatted(OAuthBearerLoginModule.class.getName()));
        return oauthClientConfig;
    }

    @NonNull
    protected Map<String, Object> getProducerConfig() {
        Map<String, Object> clientConfig = getClientConfig(BaseOauthBearerIT.TOKEN_ENDPOINT_URL);
        clientConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, "producer");
        return clientConfig;
    }

    @NonNull
    protected Map<String, Object> getConsumerConfig() {
        Map<String, Object> clientConfig = getClientConfig(BaseOauthBearerIT.TOKEN_ENDPOINT_URL);
        clientConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, "consumer");
        clientConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        clientConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return clientConfig;
    }

    protected static class OauthServerContainer extends GenericContainer<OauthServerContainer> {
        protected OauthServerContainer(DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        @Override
        protected void addFixedExposedPort(int hostPort, int containerPort) {
            super.addFixedExposedPort(hostPort, containerPort);
        }
    }
}
