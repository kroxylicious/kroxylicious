/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.proxy.config2.FilesystemSnapshot;
import io.kroxylicious.proxy.filter.FilterFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FilesystemSnapshotBuilderTest {

    @TempDir
    Path configDir;

    @Test
    void writesProxyYaml() throws IOException {
        String proxyYaml = "version: v1alpha1\nvirtualClusters: []\n";

        new FilesystemSnapshotBuilder(configDir)
                .proxyConfig(proxyYaml)
                .buildPath();

        assertThat(configDir.resolve("proxy.yaml"))
                .exists()
                .hasContent(proxyYaml);
    }

    @Test
    void writesPluginInstanceYaml() throws IOException {
        String filterYaml = """
                name: my-filter
                type: com.example.MyFilter
                version: v1
                config:
                  key: value
                """;

        new FilesystemSnapshotBuilder(configDir)
                .proxyConfig("version: v1alpha1\nvirtualClusters: []\n")
                .addPluginInstance(FilterFactory.class, "my-filter", filterYaml)
                .buildPath();

        Path pluginFile = configDir.resolve("plugins.d")
                .resolve(FilterFactory.class.getName())
                .resolve("my-filter.yaml");
        assertThat(pluginFile).exists().hasContent(filterYaml);
    }

    @Test
    void writesBinaryPluginInstanceWithSidecarFile() throws IOException {
        byte[] binaryData = new byte[]{ 0x30, 0x01, 0x02, 0x03 };

        new FilesystemSnapshotBuilder(configDir)
                .proxyConfig("version: v1alpha1\nvirtualClusters: []\n")
                .addBinaryPluginInstance(
                        "io.kroxylicious.proxy.tls.KeyMaterialProvider",
                        "my-keystore",
                        "com.example.KeyStoreProvider",
                        "v1",
                        ".p12",
                        binaryData)
                .buildPath();

        Path interfaceDir = configDir.resolve("plugins.d")
                .resolve("io.kroxylicious.proxy.tls.KeyMaterialProvider");
        assertThat(interfaceDir.resolve("my-keystore.yaml"))
                .exists()
                .content().contains("name: my-keystore", "type: com.example.KeyStoreProvider", "version: v1");
        assertThat(interfaceDir.resolve("my-keystore.p12"))
                .exists()
                .hasBinaryContent(binaryData);
    }

    @Test
    void writesPasswordsYaml() throws IOException {
        new FilesystemSnapshotBuilder(configDir)
                .proxyConfig("version: v1alpha1\nvirtualClusters: []\n")
                .addPassword("my-keystore", "secret123")
                .buildPath();

        Path passwordsFile = configDir.resolve("passwords.yaml");
        assertThat(passwordsFile).exists();
        String content = Files.readString(passwordsFile);
        assertThat(content).contains("my-keystore").contains("secret123");
    }

    @Test
    void doesNotWritePasswordsYamlWhenNoPasswords() {
        new FilesystemSnapshotBuilder(configDir)
                .proxyConfig("version: v1alpha1\nvirtualClusters: []\n")
                .buildPath();

        assertThat(configDir.resolve("passwords.yaml")).doesNotExist();
    }

    @Test
    void doesNotWritePluginsDirWhenNoPlugins() {
        new FilesystemSnapshotBuilder(configDir)
                .proxyConfig("version: v1alpha1\nvirtualClusters: []\n")
                .buildPath();

        assertThat(configDir.resolve("plugins.d")).doesNotExist();
    }

    @Test
    void buildSnapshotReturnsWorkingSnapshot() {
        String filterYaml = """
                name: my-filter
                type: com.example.MyFilter
                version: v1
                config:
                  key: value
                """;

        FilesystemSnapshot snapshot = new FilesystemSnapshotBuilder(configDir)
                .proxyConfig("version: v1alpha1\nvirtualClusters: []\n")
                .addPluginInstance(FilterFactory.class, "my-filter", filterYaml)
                .addPassword("my-resource", "pass")
                .buildSnapshot();

        assertThat(snapshot.proxyConfig()).contains("version: v1alpha1");
        assertThat(snapshot.pluginInterfaces())
                .containsExactly(FilterFactory.class.getName());
        assertThat(snapshot.pluginInstances(FilterFactory.class.getName()))
                .containsExactly("my-filter");
        assertThat(snapshot.pluginInstance(
                FilterFactory.class.getName(), "my-filter").metadata().type())
                .isEqualTo("com.example.MyFilter");
        assertThat(snapshot.resourcePassword("my-resource"))
                .isEqualTo("pass".toCharArray());
        assertThat(snapshot.resourcePassword("nonexistent"))
                .isNull();
    }

    @Test
    void convenienceProxyConfigGeneratesValidYaml() {
        FilesystemSnapshot snapshot = new FilesystemSnapshotBuilder(configDir)
                .proxyConfig("localhost:9092", "my-filter")
                .buildSnapshot();

        String yaml = snapshot.proxyConfig();
        assertThat(yaml)
                .contains("version: v1alpha1")
                .contains("bootstrapServers: localhost:9092")
                .contains("name: demo")
                .contains("portIdentifiesNode:")
                .contains("defaultFilters:")
                .contains("- my-filter");
    }

    @Test
    void throwsWhenProxyConfigNotSet() {
        var builder = new FilesystemSnapshotBuilder(configDir);
        assertThatThrownBy(builder::buildPath)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("proxyConfig");
    }
}
