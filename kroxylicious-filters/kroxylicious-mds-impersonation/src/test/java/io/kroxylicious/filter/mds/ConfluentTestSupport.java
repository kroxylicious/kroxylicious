/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.testing.kms.tls.TlsHttpClientConfigurator;

final class ConfluentTestSupport {
    private ConfluentTestSupport() {
    }

    static Map<String, Object> clientProperties(Path generated, String user) throws Exception {
        var properties = new HashMap<String, Object>();
        properties.put("bootstrap.servers", System.getProperty("mds.integration.bootstrap", "localhost:19092"));
        properties.put("client.id", "same-client-id");
        properties.put("security.protocol", "SSL");
        properties.put("ssl.truststore.type", "PEM");
        properties.put("ssl.truststore.certificates", Files.readString(generated.resolve("server-ca.crt")));
        properties.put("ssl.keystore.type", "PEM");
        properties.put("ssl.keystore.key", Files.readString(generated.resolve(user + ".key")));
        properties.put("ssl.keystore.certificate.chain", Files.readString(generated.resolve(user + ".crt")));
        return properties;
    }

    static MdsToken token(Path generated) throws Exception {
        return token(generated, "alice");
    }

    static MdsToken token(Path generated, String user) throws Exception {
        var tls = new Tls(new KeyPair(generated.resolve("proxy.key").toString(), generated.resolve("proxy.crt").toString(), null),
                new TrustStore(generated.resolve("server-ca.crt").toString(), null, "PEM"), null, null);
        var config = new MdsImpersonationConfig(URI.create(System.getProperty("mds.integration.mds", "https://localhost:18090") + "/security/1.0/impersonate"),
                tls, null, null);
        var http = new TlsHttpClientConfigurator(tls).apply(HttpClient.newBuilder()).build();
        try (var client = new MdsClient(http, config)) {
            return client.impersonate(user).toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }
}
