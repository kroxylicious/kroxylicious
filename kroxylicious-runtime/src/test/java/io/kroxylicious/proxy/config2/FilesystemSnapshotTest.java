/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FilesystemSnapshotTest {

    @TempDir
    Path tempDir;

    @Test
    void readsProxyConfig() throws IOException {
        Files.writeString(tempDir.resolve("proxy.yaml"), "version: v1\n");

        var snapshot = new FilesystemSnapshot(tempDir);
        assertThat(snapshot.proxyConfig()).isEqualTo("version: v1\n");
    }

    @Test
    void throwsWhenProxyConfigMissing() {
        var snapshot = new FilesystemSnapshot(tempDir);
        assertThatThrownBy(snapshot::proxyConfig)
                .isInstanceOf(UncheckedIOException.class);
    }

    @Test
    void returnsEmptyPluginInterfacesWhenNoPluginsDir() throws IOException {
        Files.writeString(tempDir.resolve("proxy.yaml"), "version: v1\n");

        var snapshot = new FilesystemSnapshot(tempDir);
        assertThat(snapshot.pluginInterfaces()).isEmpty();
    }

    @Test
    void listsPluginInterfaces() throws IOException {
        Path pluginsDir = tempDir.resolve("plugins.d");
        Files.createDirectories(pluginsDir.resolve("com.example.A"));
        Files.createDirectories(pluginsDir.resolve("com.example.B"));

        var snapshot = new FilesystemSnapshot(tempDir);
        assertThat(snapshot.pluginInterfaces())
                .containsExactlyInAnyOrder("com.example.A", "com.example.B");
    }

    @Test
    void listsPluginInstances() throws IOException {
        Path interfaceDir = tempDir.resolve("plugins.d").resolve("com.example.A");
        Files.createDirectories(interfaceDir);
        Files.writeString(interfaceDir.resolve("inst1.yaml"), "name: inst1\nconfig: {}");
        Files.writeString(interfaceDir.resolve("inst2.yaml"), "name: inst2\nconfig: {}");

        var snapshot = new FilesystemSnapshot(tempDir);
        assertThat(snapshot.pluginInstances("com.example.A"))
                .containsExactlyInAnyOrder("inst1", "inst2");
    }

    @Test
    void returnsEmptyInstancesForMissingInterface() {
        var snapshot = new FilesystemSnapshot(tempDir);
        assertThat(snapshot.pluginInstances("com.example.Missing")).isEmpty();
    }

    @Test
    void returnsPluginInstanceMetadata() throws IOException {
        Path interfaceDir = tempDir.resolve("plugins.d").resolve("com.example.A");
        Files.createDirectories(interfaceDir);
        Files.writeString(interfaceDir.resolve("my-inst.yaml"),
                "name: my-inst\ntype: com.example.Impl\nversion: v1\nshared: true\n");

        var snapshot = new FilesystemSnapshot(tempDir);
        PluginInstanceContent content = snapshot.pluginInstance("com.example.A", "my-inst");

        assertThat(content.metadata().name()).isEqualTo("my-inst");
        assertThat(content.metadata().type()).isEqualTo("com.example.Impl");
        assertThat(content.metadata().version()).isEqualTo("v1");
        assertThat(content.metadata().shared()).isTrue();
        assertThat(content.metadata().generation()).isPositive();
    }

    @Test
    void returnsYamlDataBytesWhenNoSidecarFile() throws IOException {
        Path interfaceDir = tempDir.resolve("plugins.d").resolve("com.example.A");
        Files.createDirectories(interfaceDir);
        String yaml = "name: my-inst\ntype: Foo\nversion: v1\n";
        Files.writeString(interfaceDir.resolve("my-inst.yaml"), yaml);

        var snapshot = new FilesystemSnapshot(tempDir);
        PluginInstanceContent content = snapshot.pluginInstance("com.example.A", "my-inst");

        assertThat(new String(content.data(), StandardCharsets.UTF_8)).isEqualTo(yaml);
    }

    @Test
    void returnsSidecarDataBytesWithP12Extension() throws IOException {
        Path interfaceDir = tempDir.resolve("plugins.d").resolve("com.example.A");
        Files.createDirectories(interfaceDir);
        Files.writeString(interfaceDir.resolve("my-inst.yaml"),
                "name: my-inst\ntype: com.example.Impl\nversion: v1\n");
        byte[] binaryData = { 0x30, (byte) 0x82, 0x01, 0x22 };
        Files.write(interfaceDir.resolve("my-inst.p12"), binaryData);

        var snapshot = new FilesystemSnapshot(tempDir);
        PluginInstanceContent content = snapshot.pluginInstance("com.example.A", "my-inst");

        assertThat(content.data()).isEqualTo(binaryData);
    }

    @Test
    void returnsSidecarDataBytesWithJksExtension() throws IOException {
        Path interfaceDir = tempDir.resolve("plugins.d").resolve("com.example.A");
        Files.createDirectories(interfaceDir);
        Files.writeString(interfaceDir.resolve("my-inst.yaml"),
                "name: my-inst\ntype: com.example.Impl\nversion: v1\n");
        byte[] binaryData = { (byte) 0xFE, (byte) 0xED, (byte) 0xFE, (byte) 0xED };
        Files.write(interfaceDir.resolve("my-inst.jks"), binaryData);

        var snapshot = new FilesystemSnapshot(tempDir);
        PluginInstanceContent content = snapshot.pluginInstance("com.example.A", "my-inst");

        assertThat(content.data()).isEqualTo(binaryData);
    }

    @Test
    void generationReflectsLatestMtimeIncludingSidecarFile() throws IOException {
        Path interfaceDir = tempDir.resolve("plugins.d").resolve("com.example.A");
        Files.createDirectories(interfaceDir);
        Path yamlFile = interfaceDir.resolve("my-inst.yaml");
        Files.writeString(yamlFile, "name: my-inst\ntype: com.example.Impl\nversion: v1\n");
        Path dataFile = interfaceDir.resolve("my-inst.p12");
        Files.write(dataFile, new byte[]{ 1, 2, 3 });

        var snapshot = new FilesystemSnapshot(tempDir);
        PluginInstanceContent content = snapshot.pluginInstance("com.example.A", "my-inst");

        long yamlMtime = Files.getLastModifiedTime(yamlFile).toMillis();
        long dataMtime = Files.getLastModifiedTime(dataFile).toMillis();
        assertThat(content.metadata().generation()).isEqualTo(Math.max(yamlMtime, dataMtime));
    }

    @Test
    void throwsWhenMultipleSidecarFilesExist() throws IOException {
        Path interfaceDir = tempDir.resolve("plugins.d").resolve("com.example.A");
        Files.createDirectories(interfaceDir);
        Files.writeString(interfaceDir.resolve("my-inst.yaml"),
                "name: my-inst\ntype: com.example.Impl\nversion: v1\n");
        Files.write(interfaceDir.resolve("my-inst.p12"), new byte[]{ 1 });
        Files.write(interfaceDir.resolve("my-inst.jks"), new byte[]{ 2 });

        var snapshot = new FilesystemSnapshot(tempDir);
        assertThatThrownBy(() -> snapshot.pluginInstance("com.example.A", "my-inst"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiple sidecar files");
    }

    @Test
    void throwsForMissingPluginInstance() {
        var snapshot = new FilesystemSnapshot(tempDir);
        assertThatThrownBy(() -> snapshot.pluginInstance("com.example.A", "missing"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwsWhenPluginInstanceMissingName() throws IOException {
        Path interfaceDir = tempDir.resolve("plugins.d").resolve("com.example.A");
        Files.createDirectories(interfaceDir);
        Files.writeString(interfaceDir.resolve("my-inst.yaml"), "type: Foo\nversion: v1\n");

        var snapshot = new FilesystemSnapshot(tempDir);
        assertThatThrownBy(() -> snapshot.pluginInstance("com.example.A", "my-inst"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not contain a 'name' field");
    }

    @Test
    void throwsWhenPluginInstanceNameMismatch() throws IOException {
        Path interfaceDir = tempDir.resolve("plugins.d").resolve("com.example.A");
        Files.createDirectories(interfaceDir);
        Files.writeString(interfaceDir.resolve("my-inst.yaml"), "name: wrong-name\ntype: Foo\nversion: v1\n");

        var snapshot = new FilesystemSnapshot(tempDir);
        assertThatThrownBy(() -> snapshot.pluginInstance("com.example.A", "my-inst"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("wrong-name")
                .hasMessageContaining("my-inst");
    }

    @Test
    void ignoresNonYamlFiles() throws IOException {
        Path interfaceDir = tempDir.resolve("plugins.d").resolve("com.example.A");
        Files.createDirectories(interfaceDir);
        Files.writeString(interfaceDir.resolve("inst1.yaml"), "name: inst1\nconfig: {}");
        Files.writeString(interfaceDir.resolve("README.md"), "not a plugin");

        var snapshot = new FilesystemSnapshot(tempDir);
        assertThat(snapshot.pluginInstances("com.example.A"))
                .containsExactly("inst1");
    }

    @Test
    void returnsNullPasswordWhenNoPasswordsFile() {
        var snapshot = new FilesystemSnapshot(tempDir);
        assertThat(snapshot.resourcePassword("anything")).isNull();
    }

    @Test
    void returnsPasswordFromPasswordsYaml() throws IOException {
        Files.writeString(tempDir.resolve("passwords.yaml"),
                "my-keystore: changeit\nmy-truststore: secret123\n");

        var snapshot = new FilesystemSnapshot(tempDir);
        assertThat(snapshot.resourcePassword("my-keystore")).isEqualTo("changeit".toCharArray());
        assertThat(snapshot.resourcePassword("my-truststore")).isEqualTo("secret123".toCharArray());
        assertThat(snapshot.resourcePassword("unknown")).isNull();
    }
}
