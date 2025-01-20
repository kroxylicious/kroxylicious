/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

public record TlsSecrets(Path path, String volumeName, String secretName) {

    private static final Path TLS_SECRETS_ROOT_DIR = Path.of("/opt/kroxylicious/secrets/tls");
    private static final String CERTIFICATE = "tls.crt";
    private static final String KEY = "tls.key";

    public static List<TlsSecrets> tlsSecretsFor(KafkaProxy proxy) {
        final AtomicInteger counter = new AtomicInteger(0);
        return proxy.getSpec().getListeners().stream().flatMap(listeners -> listeners.getTls().getCertificateRefs().stream().map(cert -> {
            Path resolve = TLS_SECRETS_ROOT_DIR.resolve(listeners.getName()).resolve(cert.getName());
            return new TlsSecrets(resolve, "tls-secrets-" + counter.getAndIncrement(), cert.getName());
        })).toList();
    }

    public Path privateKeyPath() {
        return path.resolve(KEY);
    }

    public Path certificatePath() {
        return path.resolve(CERTIFICATE);
    }
}
