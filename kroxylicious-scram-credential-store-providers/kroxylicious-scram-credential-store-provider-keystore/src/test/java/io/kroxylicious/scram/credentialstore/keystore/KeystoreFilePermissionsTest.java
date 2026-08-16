/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.keystore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import io.kroxylicious.scram.credentialstore.CredentialServiceUnavailableException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class KeystoreFilePermissionsTest {

    @Test
    @SetEnvironmentVariable(key = KeystoreFilePermissions.PERMISSION_CHECK_ENV_VAR, value = "0640")
    void shouldAllowGroupReadableFileInRelaxedMode(@TempDir Path tempDir) throws IOException {
        // Given
        Path file = createFile(tempDir);
        assumePosixPermissions(file);
        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-r-----"));

        // When/Then
        assertThatCode(() -> KeystoreFilePermissions.checkForCredentialStore(file))
                .doesNotThrowAnyException();
    }

    @Test
    @SetEnvironmentVariable(key = KeystoreFilePermissions.PERMISSION_CHECK_ENV_VAR, value = "0640")
    void shouldRejectGroupWritableFileEvenInRelaxedMode(@TempDir Path tempDir) throws IOException {
        // Given
        Path file = createFile(tempDir);
        assumePosixPermissions(file);
        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw--w----"));

        // When/Then
        assertThatThrownBy(() -> KeystoreFilePermissions.checkForCredentialStore(file))
                .isInstanceOf(CredentialServiceUnavailableException.class)
                .hasMessageContaining("insecure permissions");
    }

    @Test
    @SetEnvironmentVariable(key = KeystoreFilePermissions.PERMISSION_CHECK_ENV_VAR, value = "unrecognized-value")
    void shouldUseStrictModeForUnrecognizedEnvVarValue(@TempDir Path tempDir) throws IOException {
        // Given
        Path file = createFile(tempDir);
        assumePosixPermissions(file);
        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-r-----"));

        // When/Then - group-readable is insecure in strict mode
        assertThatThrownBy(() -> KeystoreFilePermissions.checkForCredentialStore(file))
                .isInstanceOf(CredentialServiceUnavailableException.class)
                .hasMessageContaining("insecure permissions");
    }

    @Test
    void ensureOwnerOnlyBeforeWriteShouldRejectExistingFileWithGroupReadable(@TempDir Path tempDir) throws IOException {
        // Given
        Path file = createFile(tempDir);
        assumePosixPermissions(file);
        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-r-----"));

        // When/Then
        assertThatThrownBy(() -> KeystoreFilePermissions.ensureOwnerOnlyBeforeWrite(file))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("insecure permissions");
    }

    @Test
    void ensureOwnerOnlyBeforeWriteShouldRejectExistingFileWithWorldReadable(@TempDir Path tempDir) throws IOException {
        // Given
        Path file = createFile(tempDir);
        assumePosixPermissions(file);
        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-r--r--"));

        // When/Then
        assertThatThrownBy(() -> KeystoreFilePermissions.ensureOwnerOnlyBeforeWrite(file))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("insecure permissions");
    }

    @Test
    void ensureOwnerOnlyBeforeWriteShouldAcceptExistingFileWithOwnerOnly(@TempDir Path tempDir) throws IOException {
        // Given
        Path file = createFile(tempDir);
        assumePosixPermissions(file);
        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));

        // When/Then
        assertThatCode(() -> KeystoreFilePermissions.ensureOwnerOnlyBeforeWrite(file))
                .doesNotThrowAnyException();
    }

    @Test
    void ensureOwnerOnlyBeforeWriteShouldCreateNewFileWithOwnerOnlyPermissions(@TempDir Path tempDir) throws IOException {
        // Given
        Path file = tempDir.resolve("new-keystore.p12");
        assumePosixPermissions(tempDir);

        // When
        KeystoreFilePermissions.ensureOwnerOnlyBeforeWrite(file);

        // Then
        assertThat(Files.exists(file)).isTrue();
        assertThat(Files.getPosixFilePermissions(file))
                .isEqualTo(PosixFilePermissions.fromString("rw-------"));
    }

    @Test
    void checkForCredentialStoreShouldReturnWithoutErrorWhenFileDoesNotExist(@TempDir Path tempDir) {
        // Given
        Path nonExistentFile = tempDir.resolve("does-not-exist.p12");

        // When/Then
        assertThatCode(() -> KeystoreFilePermissions.checkForCredentialStore(nonExistentFile))
                .doesNotThrowAnyException();
    }

    @Test
    void checkForCredentialStoreShouldAllowOwnerOnlyFileInStrictMode(@TempDir Path tempDir) throws IOException {
        // Given
        Path file = createFile(tempDir);
        assumePosixPermissions(file);
        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));

        // When/Then
        assertThatCode(() -> KeystoreFilePermissions.checkForCredentialStore(file))
                .doesNotThrowAnyException();
    }

    private static Path createFile(Path dir) throws IOException {
        return Files.createTempFile(dir, "keystore-perm-test", ".tmp");
    }

    private static void assumePosixPermissions(Path path) {
        assumeTrue(
                Files.getFileAttributeView(path, PosixFileAttributeView.class) != null,
                "POSIX file permissions not supported on this platform");
    }
}
