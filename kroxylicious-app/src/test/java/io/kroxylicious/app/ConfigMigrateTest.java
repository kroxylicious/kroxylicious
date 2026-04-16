/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.app;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;

import picocli.CommandLine;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigMigrateTest {

    private static final YAMLMapper YAML_MAPPER = new YAMLMapper();

    private CommandLine cmd;
    private StringWriter soutWriter;
    private StringWriter serrWriter;

    @BeforeEach
    void setup() {
        ConfigMigrate migrate = new ConfigMigrate();
        soutWriter = new StringWriter();
        serrWriter = new StringWriter();
        cmd = new CommandLine(migrate);
        cmd.setOut(new PrintWriter(soutWriter));
        cmd.setErr(new PrintWriter(serrWriter));
    }

    @Test
    void shouldRejectMissingInputFile(@TempDir Path dir) {
        Path nonExistent = dir.resolve("does-not-exist.yaml");
        Path outDir = dir.resolve("out");
        int exitCode = cmd.execute("-i", nonExistent.toString(), "-o", outDir.toString());
        assertThat(exitCode).isEqualTo(1);
        assertThat(stdErr()).contains("Input file does not exist");
    }

    @Test
    void shouldRejectNonEmptyOutputDir(@TempDir Path dir) throws IOException {
        Path inputFile = writeMinimalConfig(dir);
        Path outDir = dir.resolve("out");
        Files.createDirectories(outDir);
        Files.writeString(outDir.resolve("existing.txt"), "something");

        int exitCode = cmd.execute("-i", inputFile.toString(), "-o", outDir.toString());
        assertThat(exitCode).isEqualTo(1);
        assertThat(stdErr()).contains("Output directory is not empty");
    }

    @Test
    void shouldCreateOutputDirIfAbsent(@TempDir Path dir) throws IOException {
        Path inputFile = writeMinimalConfig(dir);
        Path outDir = dir.resolve("new-output");

        int exitCode = cmd.execute("-i", inputFile.toString(), "-o", outDir.toString());
        assertThat(exitCode).isEqualTo(0);
        assertThat(outDir).isDirectory();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldMigrateMinimalConfig(@TempDir Path dir) throws IOException {
        Path inputFile = writeMinimalConfig(dir);
        Path outDir = dir.resolve("out");

        int exitCode = cmd.execute("-i", inputFile.toString(), "-o", outDir.toString());
        assertThat(exitCode).isEqualTo(0);

        Path proxyYaml = outDir.resolve("proxy.yaml");
        assertThat(proxyYaml).isRegularFile();

        Map<String, Object> proxy = YAML_MAPPER.readValue(proxyYaml.toFile(), Map.class);
        assertThat(proxy).containsKey("version");
        assertThat(proxy.get("version")).isEqualTo("v1alpha1");
        assertThat(proxy).containsKey("virtualClusters");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldMigrateConfigWithRecordEncryptionFilter(@TempDir Path dir) throws IOException {
        String yaml = """
                filterDefinitions:
                  - name: encrypt
                    type: RecordEncryption
                    config:
                      kms: VaultKmsService
                      kmsConfig:
                        vaultTransitEngineUrl: http://vault:8200/v1/transit
                        vaultToken:
                          password: my-token
                      selector: TemplateKekSelector
                      selectorConfig:
                        template: "KEK_${topicName}"
                defaultFilters:
                  - encrypt
                virtualClusters:
                  - name: demo
                    targetCluster:
                      bootstrapServers: localhost:9092
                    gateways:
                      - name: mygateway
                        portIdentifiesNode:
                          bootstrapAddress: localhost:9192
                """;
        Path inputFile = dir.resolve("config.yaml");
        Files.writeString(inputFile, yaml);
        Path outDir = dir.resolve("out");

        int exitCode = cmd.execute("-i", inputFile.toString(), "-o", outDir.toString());
        assertThat(exitCode).isEqualTo(0);

        // Check filter plugin file
        Path filterFile = outDir.resolve("plugins.d/io.kroxylicious.proxy.filter.FilterFactory/encrypt.yaml");
        assertThat(filterFile).isRegularFile();
        Map<String, Object> filterContent = YAML_MAPPER.readValue(filterFile.toFile(), Map.class);
        assertThat(filterContent.get("name")).isEqualTo("encrypt");
        assertThat(filterContent.get("type")).isEqualTo("io.kroxylicious.filter.encryption.RecordEncryption");
        assertThat(filterContent).containsKey("version");
        assertThat(filterContent).containsKey("config");

        Map<String, Object> filterConfig = (Map<String, Object>) filterContent.get("config");
        // The nested plugins should be replaced with instance name strings
        assertThat(filterConfig.get("kms")).isEqualTo("encrypt-kms");
        assertThat(filterConfig.get("selector")).isEqualTo("encrypt-selector");

        // Check KMS plugin file was extracted
        Path kmsFile = outDir.resolve("plugins.d/io.kroxylicious.kms.service.KmsService/encrypt-kms.yaml");
        assertThat(kmsFile).isRegularFile();
        Map<String, Object> kmsContent = YAML_MAPPER.readValue(kmsFile.toFile(), Map.class);
        assertThat(kmsContent.get("name")).isEqualTo("encrypt-kms");
        assertThat(kmsContent.get("type")).isEqualTo("io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsService");
        assertThat(kmsContent).containsKey("config");
        Map<String, Object> kmsConfig = (Map<String, Object>) kmsContent.get("config");
        assertThat(kmsConfig).containsKey("vaultTransitEngineUrl");

        // Check KekSelector plugin file was extracted
        Path selectorFile = outDir.resolve(
                "plugins.d/io.kroxylicious.filter.encryption.config.KekSelectorService/encrypt-selector.yaml");
        assertThat(selectorFile).isRegularFile();
        Map<String, Object> selectorContent = YAML_MAPPER.readValue(selectorFile.toFile(), Map.class);
        assertThat(selectorContent.get("name")).isEqualTo("encrypt-selector");
        assertThat(selectorContent.get("type")).isEqualTo("io.kroxylicious.filter.encryption.TemplateKekSelector");
        assertThat(selectorContent).containsKey("config");

        // Check proxy.yaml
        Path proxyYaml = outDir.resolve("proxy.yaml");
        Map<String, Object> proxy = YAML_MAPPER.readValue(proxyYaml.toFile(), Map.class);
        assertThat(proxy.get("defaultFilters")).asList().containsExactly("encrypt");
    }

    @Test
    void findVersionedVersionPrefersNonEmpty() {
        Map<String, Class<?>> versions = new LinkedHashMap<>();
        versions.put("", String.class);
        versions.put("v1beta1", Integer.class);
        assertThat(ConfigMigrate.findVersionedVersion(versions)).isEqualTo("v1beta1");
    }

    @Test
    void findVersionedVersionDefaultsToV1alpha1WhenAllEmpty() {
        Map<String, Class<?>> versions = new LinkedHashMap<>();
        versions.put("", String.class);
        assertThat(ConfigMigrate.findVersionedVersion(versions)).isEqualTo("v1alpha1");
    }

    @Test
    void generateMicrometerNameSingleInstance() {
        assertThat(ConfigMigrate.generateMicrometerName("com.example.CommonTagsHook", 0, 1))
                .isEqualTo("common-tags-hook");
    }

    @Test
    void generateMicrometerNameMultipleInstances() {
        assertThat(ConfigMigrate.generateMicrometerName("com.example.CommonTagsHook", 0, 2))
                .isEqualTo("common-tags-hook-0");
        assertThat(ConfigMigrate.generateMicrometerName("com.example.CommonTagsHook", 1, 2))
                .isEqualTo("common-tags-hook-1");
    }

    @Test
    void toKebabCaseConvertsCorrectly() {
        assertThat(ConfigMigrate.toKebabCase("CommonTagsHook")).isEqualTo("common-tags-hook");
        assertThat(ConfigMigrate.toKebabCase("simple")).isEqualTo("simple");
        assertThat(ConfigMigrate.toKebabCase("ABC")).isEqualTo("a-b-c");
    }

    @Test
    void discoverNestedPluginsFindsAnnotatedPairs() {
        var pairs = ConfigMigrate.discoverNestedPlugins(TwoPluginConfig.class);
        assertThat(pairs).hasSize(2);
        assertThat(pairs.get(0).nameField()).isEqualTo("kms");
        assertThat(pairs.get(0).configField()).isEqualTo("kmsConfig");
        assertThat(pairs.get(0).pluginInterface()).isEqualTo(DummyKmsService.class);
        assertThat(pairs.get(1).nameField()).isEqualTo("selector");
        assertThat(pairs.get(1).configField()).isEqualTo("selectorConfig");
        assertThat(pairs.get(1).pluginInterface()).isEqualTo(DummyKekSelectorService.class);
    }

    @Test
    void discoverNestedPluginsReturnsEmptyForNonRecord() {
        assertThat(ConfigMigrate.discoverNestedPlugins(String.class)).isEmpty();
    }

    @Test
    void discoverNestedPluginsReturnsEmptyForRecordWithoutAnnotations() {
        assertThat(ConfigMigrate.discoverNestedPlugins(PlainConfig.class)).isEmpty();
    }

    // -- test helper records --

    // Dummy interfaces to simulate plugin annotations without pulling in real plugin dependencies
    interface DummyKmsService {
    }

    interface DummyKekSelectorService {
    }

    /** A record with two nested plugin pairs, mirroring RecordEncryptionConfig's annotation pattern. */
    record TwoPluginConfig(
                           @PluginImplName(DummyKmsService.class) String kms,
                           @PluginImplConfig(implNameProperty = "kms") Object kmsConfig,
                           @PluginImplName(DummyKekSelectorService.class) String selector,
                           @PluginImplConfig(implNameProperty = "selector") Object selectorConfig) {}

    /** A record with no plugin annotations. */
    record PlainConfig(String name, int count) {}

    // -- helpers --

    private Path writeMinimalConfig(Path dir) throws IOException {
        String yaml = """
                virtualClusters:
                  - name: demo
                    targetCluster:
                      bootstrapServers: localhost:9092
                    gateways:
                      - name: mygateway
                        portIdentifiesNode:
                          bootstrapAddress: localhost:9192
                defaultFilters: []
                """;
        Path file = dir.resolve("config.yaml");
        Files.writeString(file, yaml);
        return file;
    }

    private String stdErr() {
        return serrWriter.toString();
    }

    private String stdOut() {
        return soutWriter.toString();
    }
}
