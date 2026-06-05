/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.azure;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import io.kroxylicious.testing.kafka.common.KeystoreManager;
import io.kroxylicious.testing.kms.TestKmsFacadeException;

@SuppressWarnings("java:S2160") // equals on superclass is not intended for use
class OauthServerContainer extends GenericContainer<OauthServerContainer> {

    private static final String PKCS12_KEYSTORE_TYPE = "PKCS12";
    // mock-oauth2-server is not uploaded to docker hub, so setting the digest doesn't work when looking for the image on docker hub.
    // When this issue https://github.com/testcontainers/testcontainers-java/issues/10527 is fixed, we can use the digest here.
    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("ghcr.io/navikt/mock-oauth2-server:3.0.1");
    private static final String LOCALHOST = "localhost";
    private static final String CERT_FILE_MOUNT_PATH = "/etc/custom-certs";
    private final KeystoreTrustStorePair certs;
    private static final int INTERNAL_PORT = 8080;

    protected record KeystoreTrustStorePair(String brokerKeyStore, String keyStoreType, String clientTrustStore, String password) {}

    protected static KeystoreTrustStorePair buildKeystoreTrustStorePair() {
        var keystoreManager = new KeystoreManager();
        String dn = keystoreManager.buildDistinguishedName("test@kroxylicious.io", LOCALHOST, "KI", "kroxylicious.io", null, null, "US");
        Path keystorePath;
        try {
            var bundle = keystoreManager.createSelfSignedCertificate(keystoreManager.newCertificateBuilder(dn));
            keystorePath = keystoreManager.generateCertificateFile(bundle);
        }
        catch (Exception e) {
            throw new TestKmsFacadeException(e);
        }
        String password = keystoreManager.getPassword(keystorePath);
        // The generated JKS contains both the private key entry and the CA cert,
        // so the same file serves as both the proxy keystore and the client truststore.
        String keystore = keystorePath.toAbsolutePath().toString();
        return new KeystoreTrustStorePair(keystore, PKCS12_KEYSTORE_TYPE, keystore, password);
    }

    OauthServerContainer() {
        super(DOCKER_IMAGE_NAME);
        this.certs = buildKeystoreTrustStorePair();
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
                """.formatted(certs.password(), CERT_FILE_MOUNT_PATH, certs.keyStoreType(), certs.password());
        withEnv("JSON_CONFIG", config);
        withEnv("LOG_LEVEL", "DEBUG"); // required to for the startup message to be logged.
        withCopyFileToContainer(MountableFile.forHostPath(certs.brokerKeyStore()), CERT_FILE_MOUNT_PATH);
    }

    public String getTrustStoreLocation() {
        return certs.clientTrustStore();
    }

    public String getTrustStorePassword() {
        return certs.password();
    }

    public String getTrustStoreType() {
        return certs.keyStoreType();
    }

    public URI getBaseUri() {
        Integer mappedPort = getMappedPort(INTERNAL_PORT);
        if (mappedPort == null) {
            throw new IllegalStateException("oauth mock - mappedPort is null for " + INTERNAL_PORT);
        }
        return URI.create("https://" + LOCALHOST + ":" + mappedPort);
    }
}
