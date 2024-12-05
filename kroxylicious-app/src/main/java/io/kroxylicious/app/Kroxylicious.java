/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.app;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.KafkaProxy;
import io.kroxylicious.proxy.ProxyEnvironment;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

/**
 * Kroxylicious application entrypoint
 */
@Command(name = "kroxylicious", mixinStandardHelpOptions = true, versionProvider = Kroxylicious.VersionProvider.class, description = "A customizable wire protocol proxy for Apache Kafka")
public class Kroxylicious implements Callable<Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger("io.kroxylicious.proxy.StartupShutdownLogger");
    private static final String UNKNOWN = "unknown";
    public static final String KROXYLICIOUS_ENVIRONMENT = "KROXYLICIOUS_ENVIRONMENT";
    private final KafkaProxyBuilder proxyBuilder;

    interface KafkaProxyBuilder {
        KafkaProxy build(PluginFactoryRegistry registry, Configuration config, ProxyEnvironment environment);
    }

    Kroxylicious() {
        this(KafkaProxy::new);
    }

    Kroxylicious(KafkaProxyBuilder proxyBuilder) {
        this.proxyBuilder = proxyBuilder;
    }

    @Spec
    private CommandSpec spec;

    @Option(names = { "-c", "--config" }, description = "name of the configuration file", required = true)
    private File configFile;

    @Override
    public Integer call() throws Exception {
        if (!configFile.exists()) {
            throw new ParameterException(spec.commandLine(), String.format("Given configuration file does not exist: %s", configFile.toPath().toAbsolutePath()));
        }

        ConfigParser configParser = new ConfigParser();
        try (InputStream stream = Files.newInputStream(configFile.toPath())) {

            Configuration config = configParser.parseConfiguration(stream);
            ProxyEnvironment proxyEnvironment = getEnvironment();
            printBannerAndVersions(proxyEnvironment);
            try (KafkaProxy kafkaProxy = proxyBuilder.build(configParser, config, proxyEnvironment)) {
                kafkaProxy.startup();
                kafkaProxy.block();
            }
        }
        catch (Exception e) {
            LOGGER.error("Exception on startup", e);
            throw e;
        }

        return 0;
    }

    private static ProxyEnvironment getEnvironment() {
        String environmentString = System.getProperty(KROXYLICIOUS_ENVIRONMENT,
                System.getenv().getOrDefault(KROXYLICIOUS_ENVIRONMENT, ProxyEnvironment.PRODUCTION.name()));
        return ProxyEnvironment.valueOf(environmentString);
    }

    private static void printBannerAndVersions(ProxyEnvironment proxyEnvironment) throws Exception {
        new BannerLogger().log();
        String[] versions = new VersionProvider().getVersion();
        for (String version : versions) {
            LOGGER.info("{}", version);
        }
        LOGGER.info("environment: {}", proxyEnvironment.name());
        if (proxyEnvironment == ProxyEnvironment.DEVELOPMENT) {
            LOGGER.warn("Warning, kroxylicious is configured for development. The proxy is permitted to apply some features that are for testing only.");
        }
        LOGGER.atInfo()
                .setMessage("Platform: Java {}({}) running on {} {}/{}")
                .addArgument(Runtime::version)
                .addArgument(() -> System.getProperty("java.vendor"))
                .addArgument(() -> System.getProperty("os.name"))
                .addArgument(() -> System.getProperty("os.version"))
                .addArgument(() -> System.getProperty("os.arch"))
                .log();
    }

    /**
     * Kroxylicious entry point
     * @param args args
     */
    public static void main(String... args) {
        int exitCode = new CommandLine(new Kroxylicious()).execute(args);
        System.exit(exitCode);
    }

    static class VersionProvider implements CommandLine.IVersionProvider {
        @Override
        public String[] getVersion() throws Exception {
            try (InputStream resource = this.getClass().getClassLoader().getResourceAsStream("META-INF/metadata.properties")) {
                if (resource != null) {
                    Properties properties = new Properties();
                    properties.load(resource);
                    String version = properties.getProperty("kroxylicious.version", UNKNOWN);
                    String commitId = properties.getProperty("git.commit.id", UNKNOWN);
                    String commitMessage = properties.getProperty("git.commit.message.short", UNKNOWN);
                    return new String[]{ "kroxylicious: " + version, "commit id: " + commitId, "commit message: " + commitMessage };
                }
            }
            return new String[]{ UNKNOWN };
        }
    }
}
