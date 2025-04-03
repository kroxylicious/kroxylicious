/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

class TestFilesTest {

    @Test
    void testSubDirectories() {
        List<Path> paths = TestFiles.subDirectoriesForTest(TestFilesTest.class);
        Path one = Path.of("target", "test-classes", "TestFilesTest", "sub1");
        Path two = Path.of("target", "test-classes", "TestFilesTest", "sub2");
        assertThat(paths).containsExactlyInAnyOrder(one, two);
    }

    @Test
    void testSubDirectoriesWhenEmpty(@TempDir Path tempDir) {
        List<Path> paths = TestFiles.subDirectories(tempDir);
        assertThat(paths).isEmpty();
    }

    @Test
    void testSubDirectoriesWhenOnlyContainsFile(@TempDir Path tempDir) throws IOException {
        Files.writeString(tempDir.resolve("file"), "hello");
        List<Path> paths = TestFiles.subDirectories(tempDir);
        assertThat(paths).isEmpty();
    }

    @Test
    void testSubDirectoriesWithSingleSubdir(@TempDir Path tempDir) throws IOException {
        Path subdir = tempDir.resolve("file");
        Files.createDirectory(subdir);
        List<Path> paths = TestFiles.subDirectories(tempDir);
        assertThat(paths).containsExactly(subdir);
    }

    @Test
    void childFilesMatchingWithEmptyDir(@TempDir Path tempDir) throws IOException {
        HashSet<Path> paths = TestFiles.childFilesMatching(tempDir, "any");
        assertThat(paths).isEmpty();
    }

    @Test
    void childFilesExactMatching(@TempDir Path tempDir) throws IOException {
        Files.writeString(tempDir.resolve("a"), "a");
        Files.writeString(tempDir.resolve("aa"), "aa");
        HashSet<Path> paths = TestFiles.childFilesMatching(tempDir, "a");
        assertThat(paths).containsExactly(tempDir.resolve("a"));
    }

    static Stream<Arguments> childFilesMatchingGlob() {
        Arguments all = Arguments.of(Set.of("a", "b"), "*", Set.of("a", "b"));
        Arguments prefix = Arguments.of(Set.of("aa", "ab"), "*b", Set.of("ab"));
        Arguments suffix = Arguments.of(Set.of("aa", "ba"), "a*", Set.of("aa"));
        Arguments extension = Arguments.of(Set.of("a.yaml", "b.yaml"), "*.yaml", Set.of("a.yaml", "b.yaml"));
        return Stream.of(all, prefix, suffix, extension);
    }

    @MethodSource
    @ParameterizedTest
    void childFilesMatchingGlob(Set<String> filenames, String glob, Set<String> expectedToResolve, @TempDir Path tempDir) throws IOException {
        filenames.forEach(s -> writeFile(tempDir, s));
        HashSet<Path> paths = TestFiles.childFilesMatching(tempDir, glob);
        Set<Path> expectedPaths = expectedToResolve.stream().map(tempDir::resolve).collect(toSet());
        assertThat(paths).containsExactlyInAnyOrderElementsOf(expectedPaths);
    }

    @Test
    void recursiveFilesForTestingEmpty(@TempDir Path tempDir) throws IOException {
        List<Path> paths = TestFiles.recursiveFilesInDirectory("*", tempDir);
        assertThat(paths).isEmpty();
    }

    @Test
    void recursiveFilesForTesting(@TempDir Path tempDir) throws IOException {
        Path rootFile = tempDir.resolve("fileInRoot");
        Files.writeString(rootFile, "hello");
        Path dirA = tempDir.resolve("a");
        Files.createDirectory(dirA);
        Path fileA = dirA.resolve("fileA");
        Files.writeString(fileA, "a");
        Path dirB = tempDir.resolve("b");
        Files.createDirectory(dirB);
        Path fileB = dirB.resolve("fileB");
        Files.writeString(fileB, "b");
        List<Path> paths = TestFiles.recursiveFilesInDirectory("*", tempDir);
        assertThat(paths).containsExactlyInAnyOrder(fileA, fileB, rootFile);
    }

    @Test
    void recursiveFilesForTestingGlob(@TempDir Path tempDir) throws IOException {
        Path rootFile = tempDir.resolve("fileInRoot");
        Files.writeString(rootFile, "hello");
        Path dirA = tempDir.resolve("a");
        Files.createDirectory(dirA);
        Path fileA = dirA.resolve("fileA");
        Files.writeString(fileA, "a");
        Path dirB = tempDir.resolve("b");
        Files.createDirectory(dirB);
        Path fileB = dirB.resolve("fileB");
        Files.writeString(fileB, "b");
        List<Path> paths = TestFiles.recursiveFilesInDirectory("*A", tempDir);
        assertThat(paths).containsExactlyInAnyOrder(fileA);
    }

    private static void writeFile(Path tempDir, String filename) {
        try {
            Files.writeString(tempDir.resolve(filename), "arbitrary");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
