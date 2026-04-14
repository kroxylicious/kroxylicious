/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Entrypoint for the Kroxylicious sidecar injection webhook.
 */
public class WebhookMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebhookMain.class);

    private static final String BIND_ADDRESS_VAR = "BIND_ADDRESS";
    private static final String TLS_CERT_PATH_VAR = "TLS_CERT_PATH";
    private static final String TLS_KEY_PATH_VAR = "TLS_KEY_PATH";
    private static final String KROXYLICIOUS_IMAGE_VAR = "KROXYLICIOUS_IMAGE";

    private static final String DEFAULT_BIND_ADDRESS = "0.0.0.0:8443";
    private static final String DEFAULT_CERT_PATH = "/etc/webhook/tls/tls.crt";
    private static final String DEFAULT_KEY_PATH = "/etc/webhook/tls/tls.key";

    private final WebhookServer server;
    private final SidecarConfigResolver configResolver;
    private final KubernetesClient kubeClient;

    WebhookMain(
                @NonNull WebhookServer server,
                @NonNull SidecarConfigResolver configResolver,
                @NonNull KubernetesClient kubeClient) {
        this.server = server;
        this.configResolver = configResolver;
        this.kubeClient = kubeClient;
    }

    public static void main(String[] args) {
        try {
            WebhookMain webhook = create();
            webhook.start();
        }
        catch (Exception e) {
            LOGGER.atError()
                    .setCause(e)
                    .log("Webhook has thrown exception during startup, will now exit");
            System.exit(1);
        }
    }

    @NonNull
    static WebhookMain create() throws IOException, GeneralSecurityException {
        Map<String, String> env = System.getenv();

        InetSocketAddress bindAddress = parseBindAddress(env);
        Path certPath = Path.of(env.getOrDefault(TLS_CERT_PATH_VAR, DEFAULT_CERT_PATH));
        Path keyPath = Path.of(env.getOrDefault(TLS_KEY_PATH_VAR, DEFAULT_KEY_PATH));
        String proxyImage = requiredEnv(env, KROXYLICIOUS_IMAGE_VAR);

        KubernetesClient kubeClient = new KubernetesClientBuilder().build();
        SidecarConfigResolver configResolver = new SidecarConfigResolver(kubeClient);
        AdmissionHandler admissionHandler = new AdmissionHandler(configResolver, proxyImage);
        WebhookServer server = new WebhookServer(bindAddress, certPath, keyPath, admissionHandler);

        return new WebhookMain(server, configResolver, kubeClient);
    }

    void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "webhook-shutdown"));
        server.start();
        LOGGER.atInfo()
                .addKeyValue("javaVersion", Runtime::version)
                .log("Webhook started");
    }

    void stop() {
        try {
            server.close();
        }
        catch (Exception e) {
            LOGGER.atError()
                    .setCause(e)
                    .log("Error stopping webhook server");
        }
        try {
            configResolver.close();
        }
        catch (Exception e) {
            LOGGER.atError()
                    .setCause(e)
                    .log("Error closing config resolver");
        }
        try {
            kubeClient.close();
        }
        catch (Exception e) {
            LOGGER.atError()
                    .setCause(e)
                    .log("Error closing Kubernetes client");
        }
        LOGGER.atInfo().log("Webhook stopped");
    }

    @NonNull
    private static InetSocketAddress parseBindAddress(@NonNull Map<String, String> env) {
        String bindAddress = env.getOrDefault(BIND_ADDRESS_VAR, DEFAULT_BIND_ADDRESS);
        HostPort hostPort = HostPort.parse(bindAddress);
        return new InetSocketAddress(hostPort.host(), hostPort.port());
    }

    @NonNull
    private static String requiredEnv(
                                      @NonNull Map<String, String> env,
                                      @NonNull String name) {
        String value = env.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Required environment variable " + name + " is not set");
        }
        return value;
    }
}
