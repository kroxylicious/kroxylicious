/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileWatcherTest {

    @TempDir
    Path tempDir;

    @TempDir
    Path differentTempDir; // some tests require multiple directories.

    @Test
    void testLifecycleContract() {
        // Given
        final var watcher = new FileWatcher();

        // Then
        assertThat(watcher.isRunning()).isFalse();

        // When
        watcher.start();

        // Then
        Awaitility.await("watcher should enter the running state")
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(watcher.isRunning()).isTrue());

        // When
        watcher.close();

        // Then
        Awaitility.await("watcher should exit the running state")
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(watcher.isRunning()).isFalse());
    }

    @Test
    void closeIsIdempotent() {
        // Given
        final var watcher = new FileWatcher();

        // When
        assertThatCode(() -> {
            watcher.close();
            watcher.close();
        })// Then
                .doesNotThrowAnyException();
    }

    @Test
    void startIsIdempotent() {
        // Given
        try (var watcher = new FileWatcher()) {

            // When
            assertThatCode(() -> {
                watcher.start();
                watcher.start();
            }) // Then
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void registeredListenersNotifiedOnEachChange() throws IOException {
        // Given
        final var tempFile = tempDir.resolve("watch-test.txt");
        Files.writeString(tempFile, "Some text", StandardOpenOption.CREATE);
        final var notificationReceived = new AtomicBoolean(false);

        try (var watcher = new FileWatcher()) {
            watcher.register(tempFile, () -> {
                notificationReceived.set(true);
            });
            watcher.start();

            // When
            Files.writeString(tempFile, "Updated value", StandardOpenOption.TRUNCATE_EXISTING);

            // Then
            Awaitility.await("file change should notify the registered listeners")
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(notificationReceived.get()).isTrue());

            notificationReceived.set(false); // reset flag

            // When
            Files.writeString(tempFile, "Updated value 2", StandardOpenOption.TRUNCATE_EXISTING);

            // Then
            Awaitility.await("file change should notify the registered listeners")
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(notificationReceived.get()).isTrue());
        }
    }

    @Test
    void registeredListenersNotified() throws IOException {
        final var tempFile = tempDir.resolve("watch-test.txt");
        Files.writeString(tempFile, "Some text", StandardOpenOption.CREATE);
        final var notificationReceived = new AtomicBoolean(false);
        final var notificationReceived2 = new AtomicBoolean(false);

        try (var watcher = new FileWatcher()) {
            // Given
            watcher.register(tempFile, () -> {
                notificationReceived.set(true);
            });
            watcher.register(tempFile, () -> {
                notificationReceived2.set(true);
            });
            watcher.start();

            // When
            Files.writeString(tempFile, "Updated value", StandardOpenOption.TRUNCATE_EXISTING);

            // Then
            Awaitility.await("file change should notify the registered listeners")
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(notificationReceived.get() && notificationReceived2.get()).isTrue());

        }
    }

    @Test
    void registeredListenerAfterStartNotified() throws IOException {
        final var tempFile = tempDir.resolve("watch-test.txt");
        Files.writeString(tempFile, "Some text", StandardOpenOption.CREATE);
        final var notificationReceived = new AtomicBoolean(false);

        try (var watcher = new FileWatcher()) {

            watcher.start();
            // Given
            Awaitility.await("watcher should enter the running state")
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(watcher.isRunning()).isTrue());

            // When
            watcher.register(tempFile, () -> {
                notificationReceived.set(true);
            });
            Files.writeString(tempFile, "Updated value", StandardOpenOption.TRUNCATE_EXISTING);

            // Then
            Awaitility.await("file change should notify the registered listeners")
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(notificationReceived.get()).isTrue());

        }
    }

    @EnabledOnOs({ OS.LINUX, OS.MAC }) // test uses symlinks which may not be available
    @Test
    void replaceSymlink() throws IOException {

        // Given
        final var target = differentTempDir.resolve("watch-test.txt");
        final var link = tempDir.resolve("watch-test.txt");
        Files.writeString(target, "Some text", StandardOpenOption.CREATE);
        Files.createSymbolicLink(link, target);

        final var notificationReceived = new AtomicBoolean(false);

        try (var watcher = new FileWatcher()) {
            watcher.register(link, () -> {
                notificationReceived.set(true);
            });
            watcher.start();

            Awaitility.await("watcher should enter the running state")
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(watcher.isRunning()).isTrue());

            // When
            Files.delete(link);
            Files.createSymbolicLink(link, target);

            // Then
            Awaitility.await("file change should notify the registered listener")
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(notificationReceived.get()).isTrue());
        }
    }

    @EnabledOnOs({ OS.LINUX, OS.MAC }) // test uses symlinks which may not be available
    @Test
    void simulateRealK8sAtomicSymlinkSwap() throws IOException {

        // Given
        // Reproduces the actual layout kubelet creates for a mounted Secret/ConfigMap:
        // a versioned data directory, a "..data" symlink pointing at the current version,
        // and a top level leaf symlink (e.g. tls.crt) that points *through* "..data" and
        // is never itself recreated during a rotation.
        final var dataDirV1 = Files.createDirectory(tempDir.resolve("..2024_01_01_00_00_00.000000000"));
        Files.writeString(dataDirV1.resolve("tls.crt"), "Some text", StandardOpenOption.CREATE);

        final var dataSymlink = tempDir.resolve("..data");
        Files.createSymbolicLink(dataSymlink, dataDirV1.getFileName());

        final var leafLink = tempDir.resolve("tls.crt");
        Files.createSymbolicLink(leafLink, Path.of("..data").resolve("tls.crt"));

        final var notificationReceived = new AtomicBoolean(false);

        try (var watcher = new FileWatcher()) {
            watcher.register(leafLink, () -> {
                notificationReceived.set(true);
            });
            watcher.start();

            Awaitility.await("watcher should enter the running state")
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(watcher.isRunning()).isTrue());

            // When
            final var dataDirV2 = Files.createDirectory(tempDir.resolve("..2024_01_01_00_00_01.000000000"));
            Files.writeString(dataDirV2.resolve("tls.crt"), "Updated text", StandardOpenOption.CREATE);
            final var dataSymlinkTmp = tempDir.resolve("..data_tmp");
            Files.createSymbolicLink(dataSymlinkTmp, dataDirV2.getFileName());
            Files.move(dataSymlinkTmp, dataSymlink, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            Files.walk(dataDirV1).sorted(Comparator.reverseOrder()).forEach(FileWatcherTest::deleteQuietly);

            // Then
            Awaitility.await("file change should notify the registered listener")
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(notificationReceived.get()).isTrue());
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.delete(path);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void correctListenersAreNotified() throws IOException {
        final var tempFile = tempDir.resolve("watch-test.txt");
        final var tempFile2 = differentTempDir.resolve("watch-test2.txt");
        Files.writeString(tempFile, "Some text", StandardOpenOption.CREATE);
        Files.writeString(tempFile2, "Some text", StandardOpenOption.CREATE);

        final var notificationReceived = new AtomicBoolean(false);
        final var notificationReceived2 = new AtomicBoolean(false);

        try (var watcher = new FileWatcher()) {
            // Given
            watcher.register(tempFile, () -> {
                notificationReceived.set(true);
            });
            watcher.register(tempFile2, () -> {
                notificationReceived2.set(true);
            });
            watcher.start();

            // When
            Files.writeString(tempFile, "Updated value", StandardOpenOption.TRUNCATE_EXISTING);

            // Then
            Awaitility.await("file change should notify the correct registered listener")
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(notificationReceived.get()).isTrue());

            Awaitility.await("Listener for alternate temp file should not fire")
                    .during(1, TimeUnit.SECONDS)
                    .atMost(2, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(notificationReceived2.get()).isFalse());
        }
    }

    @Test
    void directoriesCannotBeRegistered() {
        try (var watcher = new FileWatcher()) {
            assertThatThrownBy(() -> watcher.register(tempDir, () -> {
            })).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void parentlessFilesCannotBeRegistered() {
        try (var watcher = new FileWatcher()) {
            final Path path = Path.of("justafilename.txt"); // create path here to ensure only the register call can throw an exception
            assertThatThrownBy(() -> watcher.register(path, () -> {
            })).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void failingHandlersTolerated() throws IOException {
        final var tempFile = tempDir.resolve("watch-test.txt");
        Files.writeString(tempFile, "Some text", StandardOpenOption.CREATE);
        final var notificationReceived = new AtomicBoolean(false);

        try (var watcher = new FileWatcher()) {
            // Given
            watcher.register(tempFile, () -> {
                throw new RuntimeException("Something went wrong");
            });
            watcher.register(tempFile, () -> {
                notificationReceived.set(true);
            });
            watcher.start();

            // When
            Files.writeString(tempFile, "Updated value", StandardOpenOption.TRUNCATE_EXISTING);

            // Then
            Awaitility.await("file change should notify the registered listeners")
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(notificationReceived.get()).isTrue());

        }
    }
}
