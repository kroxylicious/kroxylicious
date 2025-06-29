/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.app;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.KafkaProxy;
import io.kroxylicious.proxy.VersionInfo;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;
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
    private final KafkaProxyBuilder proxyBuilder;

    interface KafkaProxyBuilder {
        KafkaProxy build(PluginFactoryRegistry registry, Configuration config, Features features);
    }

    Kroxylicious() {
        this(KafkaProxy::new);
    }

    @VisibleForTesting
    Kroxylicious(KafkaProxyBuilder proxyBuilder) {
        this.proxyBuilder = proxyBuilder;
    }

    @Spec
    private @Nullable CommandSpec spec;

    @Option(names = { "-c", "--config" }, description = "name of the configuration file", required = true)
    private @Nullable File configFile;

    @Override
    public Integer call() throws Exception {
        Objects.requireNonNull(configFile, "configFile");
        if (!configFile.exists()) {
            Objects.requireNonNull(spec, "spec");
            throw new ParameterException(spec.commandLine(), String.format("Given configuration file does not exist: %s", configFile.toPath().toAbsolutePath()));
        }

        ConfigParser configParser = new ConfigParser();
        try (InputStream stream = Files.newInputStream(configFile.toPath())) {

            Configuration config = configParser.parseConfiguration(stream);
            Features features = getFeatures();
            printBannerAndVersions(features);
            try (KafkaProxy kafkaProxy = proxyBuilder.build(configParser, config, features)) {
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

    private static boolean isExplicitlyEnabled(Feature feature) {
        String variableName = "KROXYLICIOUS_UNLOCK_" + feature.name();
        String enabledString = System.getProperty(variableName, System.getenv(variableName));
        return Boolean.parseBoolean(enabledString);
    }

    private static Features getFeatures() {
        Features.FeaturesBuilder builder = Features.builder();
        Arrays.stream(Feature.values()).forEach(f -> {
            if (isExplicitlyEnabled(f)) {
                builder.enable(f);
            }
        });
        return builder.build();
    }

    private static void printBannerAndVersions(Features features) {
        new BannerLogger().log();
        String[] versions = new VersionProvider().getVersion();
        for (String version : versions) {
            LOGGER.info("{}", version);
        }
        features.warnings().forEach(LOGGER::warn);
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
        public String[] getVersion() {
            var versionInfo = VersionInfo.VERSION_INFO;
            return new String[]{ "kroxylicious: " + versionInfo.version(), "commit id: " + versionInfo.commitId() };
        }
    }
}
