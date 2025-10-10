/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.oauthbearer;

import java.net.URI;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.RestoreSystemProperties;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
public class BaseOauthBearerIT {
    private static final int OAUTH_SERVER_PORT = 28089;
    protected static final String JWKS_ENDPOINT_URL = "http://localhost:" + OAUTH_SERVER_PORT + "/default/jwks";
    private static final URI OAUTH_ENDPOINT_URL = URI.create(JWKS_ENDPOINT_URL).resolve("/");
    protected static final URI TOKEN_ENDPOINT_URL = OAUTH_ENDPOINT_URL.resolve("default/token");
    protected static final String EXPECTED_AUDIENCE = "default";
    protected static final String ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG = "org.apache.kafka.sasl.oauthbearer.allowed.urls";
    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("ghcr.io/navikt/mock-oauth2-server:3.0.0");
    private static OauthServerContainer oauthServer;
    @SaslMechanism(value = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)
    @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.jwks.endpoint.url", value = JWKS_ENDPOINT_URL)
    @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.expected.audience", value = EXPECTED_AUDIENCE)
    @BrokerConfig(name = "listener.name.external.oauthbearer.sasl.server.callback.handler.class", value = "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler")
    KafkaCluster cluster;

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
