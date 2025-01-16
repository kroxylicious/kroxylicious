/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

class TlsSecretsTest {

    private static final String LISTENER_NAME = "my-listener";
    private static final String LISTENER_NAME2 = "another-listener";
    private static final String SECRET_NAME = "certs";
    private static final String SECRET_NAME2 = "another-secret";

    @Test
    public void singleListener() {
        var proxy = new KafkaProxyBuilder()
                .withNewSpec()
                .withListeners()
                .addNewListener()
                .withName(LISTENER_NAME)
                .withProtocol("kafka-tls")
                .withPort(9092)
                .withNewTls()
                .addNewCertificateRef()
                .withKind("Secret")
                .withName(SECRET_NAME)
                .endCertificateRef()
                .endTls()
                .endListener()
                .endSpec()
                .build();
        List<TlsSecrets> tlsSecrets = TlsSecrets.tlsSecretsFor(proxy);
        TlsSecrets expected = expectedSecret(LISTENER_NAME, SECRET_NAME);
        assertThat(tlsSecrets).containsExactly(expected);
        assertNoCollisions(tlsSecrets);
    }

    @Test
    public void multipleCerts() {
        var proxy = new KafkaProxyBuilder()
                .withNewSpec()
                .withListeners()
                .addNewListener()
                .withName(LISTENER_NAME)
                .withProtocol("kafka-tls")
                .withPort(9092)
                .withNewTls()
                .addNewCertificateRef()
                .withKind("Secret")
                .withName(SECRET_NAME)
                .endCertificateRef()
                .addNewCertificateRef()
                .withKind("Secret")
                .withName(SECRET_NAME2)
                .endCertificateRef()
                .endTls()
                .endListener()
                .endSpec()
                .build();
        List<TlsSecrets> tlsSecrets = TlsSecrets.tlsSecretsFor(proxy);
        TlsSecrets first = expectedSecret(LISTENER_NAME, SECRET_NAME);
        TlsSecrets second = expectedSecret(LISTENER_NAME, SECRET_NAME2);
        assertThat(tlsSecrets).containsExactly(first, second);
        assertNoCollisions(tlsSecrets);
    }

    @Test
    public void multipleListeners() {
        var proxy = new KafkaProxyBuilder()
                .withNewSpec()
                .withListeners()
                .addNewListener()
                .withName(LISTENER_NAME)
                .withProtocol("kafka-tls")
                .withPort(9092)
                .withNewTls()
                .addNewCertificateRef()
                .withKind("Secret")
                .withName(SECRET_NAME)
                .endCertificateRef()
                .endTls()
                .endListener()
                .addNewListener()
                .withName(LISTENER_NAME2)
                .withProtocol("kafka-tls")
                .withPort(9092)
                .withNewTls()
                .addNewCertificateRef()
                .withKind("Secret")
                .withName(SECRET_NAME)
                .endCertificateRef()
                .addNewCertificateRef()
                .withKind("Secret")
                .withName(SECRET_NAME2)
                .endCertificateRef()
                .endTls()
                .endListener()
                .endSpec()
                .build();
        List<TlsSecrets> tlsSecrets = TlsSecrets.tlsSecretsFor(proxy);
        TlsSecrets first = expectedSecret(LISTENER_NAME, SECRET_NAME);
        TlsSecrets second = expectedSecret(LISTENER_NAME2, SECRET_NAME);
        TlsSecrets third = expectedSecret(LISTENER_NAME2, SECRET_NAME2);
        assertThat(tlsSecrets).containsExactly(first, second, third);
        assertNoCollisions(tlsSecrets);
    }

    private static @NonNull TlsSecrets expectedSecret(String listenerName, String secretName) {
        String mountPath = "/opt/kroxylicious/secrets/tls/" + listenerName + "/" + secretName;
        String expectedVolumeName = "tls-secrets-" + listenerName + "-" + secretName;
        return new TlsSecrets(Path.of(mountPath), expectedVolumeName, secretName);
    }

    /**
     * sanity check general property that we want all mounted secrets to have distinct mount path and volume name
     */
    private void assertNoCollisions(List<TlsSecrets> secrets) {
        int numSecrets = secrets.size();
        long distinctPaths = secrets.stream().map(TlsSecrets::certificatePath).distinct().count();
        assertThat(distinctPaths).describedAs("expect all secret paths that will be mounted into proxy container to be distinct").isEqualTo(numSecrets);
        long distinctVolumeNames = secrets.stream().map(TlsSecrets::volumeName).distinct().count();
        assertThat(distinctVolumeNames).describedAs("expect all volume names that will be mounted into proxy container to be distinct").isEqualTo(numSecrets);
    }

}
