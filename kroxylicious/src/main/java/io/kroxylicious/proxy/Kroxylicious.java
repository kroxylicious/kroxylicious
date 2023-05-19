/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

@Command(name = "kroxilicious", mixinStandardHelpOptions = true, versionProvider = Kroxylicious.VersionProvider.class, description = "A customizeable wire protocol proxy for Apache Kafka")
class Kroxylicious implements Callable<Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Kroxylicious.class);

    @Spec
    private CommandSpec spec;

    @Option(names = { "-c", "--config" }, description = "name of the configuration file", required = true)
    private File configFile;

    @Override
    public Integer call() throws Exception {
        if (!configFile.exists()) {
            throw new ParameterException(spec.commandLine(), String.format("Given configuration file does not exist: %s", configFile.toPath().toAbsolutePath()));
        }

        try (InputStream stream = Files.newInputStream(configFile.toPath())) {
            Configuration config = new ConfigParser().parseConfiguration(stream);

            KafkaProxy kafkaProxy = new KafkaProxy(config);
            kafkaProxy.startup();
            kafkaProxy.block();
        }
        catch (Exception e) {
            LOGGER.error("Exception on startup", e);
            throw e;
        }

        return 0;
    }

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
                    String version = properties.getProperty("kroxylicious.version", "unknown");
                    String apiVersion = properties.getProperty("kroxylicious.api.version", "unknown");
                    return new String[]{ "kroxylicious: " + version, "kroxylicous apis: " + apiVersion };
                }
            }
            return new String[]{ "unknown" };
        }
    }
}
