/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.app;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.KafkaProxy;
import io.kroxylicious.proxy.internal.config.Features;

import picocli.CommandLine;

import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KroxyliciousTest {

    @Mock
    KafkaProxy mockProxy = Mockito.mock(KafkaProxy.class);
    private CommandLine cmd;
    private StringWriter soutWriter;
    private StringWriter serrWriter;
    private AtomicReference<Features> features = new AtomicReference<>(null);

    @BeforeEach
    public void setup() {
        Kroxylicious app = new Kroxylicious((ffm, configuration, features) -> {
            if (!this.features.compareAndSet(null, features)) {
                throw new IllegalStateException("env already set");
            }
            return mockProxy;
        });
        soutWriter = new StringWriter();
        serrWriter = new StringWriter();
        cmd = new CommandLine(app);
        cmd.setOut(new PrintWriter(soutWriter));
        cmd.setErr(new PrintWriter(serrWriter));
    }

    @Test
    void testExitsIfConfigurationNotSet() {
        assertThat(cmd.execute()).isEqualTo(2);
        assertThat(stdErr()).contains("Missing required option: '--config=<configFile>'");
    }

    @Test
    void testVersion() {
        assertEquals(0, cmd.execute("-V"));
        assertThat(stdOut()).containsPattern(Pattern.compile("kroxylicious: \\d+\\.\\d+\\.\\d+.*"));
    }

    @Test
    void testExitsIfConfigurationNonExistent(@TempDir Path dir) {
        String nonExistent = dir.resolve(randomUUID().toString()).toString();
        assertEquals(2, cmd.execute("-c", nonExistent));
        assertThat(stdErr()).contains("Given configuration file does not exist: " + nonExistent);
    }

    @Test
    void testExitsIfConfigurationNotExpectedFormat(@TempDir Path dir) throws IOException {
        Path file = dir.resolve(randomUUID().toString());
        Files.writeString(file, "absolute garbage");
        assertEquals(1, cmd.execute("-c", file.toString()));
        assertThat(stdErr()).contains("java.lang.IllegalArgumentException: Couldn't parse configuration");
    }

    @Test
    void testKroxyliciousStartsAndThenTerminates(@TempDir Path dir) throws Exception {
        Path file = copyClasspathResourceToTempFileInDir("proxy-config.yaml", dir);
        when(mockProxy.startup()).thenReturn(mockProxy);
        doNothing().when(mockProxy).block();
        assertEquals(0, cmd.execute("-c", file.toString()));
    }

    @Test
    void testDefaultFeatures(@TempDir Path dir) throws Exception {
        Path file = copyClasspathResourceToTempFileInDir("proxy-config.yaml", dir);
        when(mockProxy.startup()).thenReturn(mockProxy);
        doNothing().when(mockProxy).block();
        assertEquals(0, cmd.execute("-c", file.toString()));
        assertThat(features).hasValue(Features.defaultFeatures());
    }

    @Test
    void testKroxyliciousExceptionOnBlock(@TempDir Path dir) throws Exception {
        Path file = copyClasspathResourceToTempFileInDir("proxy-config.yaml", dir);
        when(mockProxy.startup()).thenReturn(mockProxy);
        Mockito.doThrow(new RuntimeException("exception on block")).when(mockProxy).block();
        int execute = cmd.execute("-c", file.toString());
        assertEquals(1, execute);
        assertThat(stdErr()).contains("exception on block");
    }

    @Test
    void testKroxyliciousExceptionOnStartup(@TempDir Path dir) throws Exception {
        Path file = copyClasspathResourceToTempFileInDir("proxy-config.yaml", dir);
        when(mockProxy.startup()).thenThrow(new RuntimeException("startup blew up"));
        assertEquals(1, cmd.execute("-c", file.toString()));
        assertThat(stdErr()).contains("startup blew up");
    }

    private Path copyClasspathResourceToTempFileInDir(String name, Path dir) throws IOException {
        Path tempFile = dir.resolve(randomUUID().toString());
        try (InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream(name)) {
            byte[] bytes = requireNonNull(resourceAsStream).readAllBytes();
            Files.write(tempFile, bytes);
        }
        return tempFile;
    }

    private String stdErr() {
        return serrWriter.toString();
    }

    private String stdOut() {
        return soutWriter.toString();
    }
}
