/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.kms;

import java.net.URI;
import java.time.Duration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import io.kroxylicious.kms.service.TestKmsFacadeException;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;

class OauthServerContainer extends GenericContainer<OauthServerContainer> {

    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("ghcr.io/navikt/mock-oauth2-server:3.0.1");
    private static final String LOCALHOST = "localhost";
    private static final String CERT_FILE_MOUNT_PATH = "/etc/custom-certs";
    private final KeytoolCertificateGenerator certs;
    private static final int INTERNAL_PORT = 8080;

    private static KeytoolCertificateGenerator entraCerts() {
        try {
            KeytoolCertificateGenerator entraCertGen = new KeytoolCertificateGenerator();
            entraCertGen.generateSelfSignedCertificateEntry("webmaster@example.com", LOCALHOST, "Engineering", "kroxylicious.io", null, null, "NZ");
            entraCertGen.generateTrustStore(entraCertGen.getCertFilePath(), "website");
            return entraCertGen;
        }
        catch (Exception e) {
            throw new TestKmsFacadeException(e);
        }
    }

    OauthServerContainer() {
        super(DOCKER_IMAGE_NAME);
        this.certs = entraCerts();
        LogMessageWaitStrategy strategy = new LogMessageWaitStrategy().withRegEx(".*started server on address.*");
        strategy.withStartupTimeout(Duration.ofSeconds(10));
        setWaitStrategy(strategy);
        withExposedPorts(INTERNAL_PORT);
        withEnv("SERVER_PORT", Integer.toString(INTERNAL_PORT));
        String config = """
                    {
                    "httpServer" : {
                        "type" : "NettyWrapper",
                        "ssl" : {
                            "keyPassword" : "%s",
                            "keystoreFile" : "%s",
                            "keystoreType" : "%s",
                            "keystorePassword" : "%s"
                        }
                    }
                }
                """.formatted(certs.getPassword(), CERT_FILE_MOUNT_PATH, certs.getKeyStoreType(), certs.getPassword());
        withEnv("JSON_CONFIG", config);
        withEnv("LOG_LEVEL", "DEBUG"); // required to for the startup message to be logged.
        withCopyFileToContainer(MountableFile.forHostPath(certs.getKeyStoreLocation()), CERT_FILE_MOUNT_PATH);
    }

    @Override
    public void start() {
        super.start();
    }

    public String getTrustStoreLocation() {
        return certs.getTrustStoreLocation();
    }

    public String getTrustStorePassword() {
        return certs.getPassword();
    }

    public String getTrustStoreType() {
        return certs.getTrustStoreType();
    }

    public URI getBaseUri() {
        Integer mappedPort = getMappedPort(INTERNAL_PORT);
        if (mappedPort == null) {
            throw new IllegalStateException("oauth mock - mappedPort is null for " + INTERNAL_PORT);
        }
        return URI.create("https://" + LOCALHOST + ":" + mappedPort);
    }
}
