/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

public class TestFiles {

    @NonNull
    static HashSet<Path> childFilesMatching(
                                            Path testDir,
                                            String glob)
            throws IOException {
        return StreamSupport.stream(Files.newDirectoryStream(testDir, glob).spliterator(), false)
                .filter(Files::isRegularFile)
                .collect(Collectors.toCollection(HashSet::new));
    }

    static List<Path> subDirectoriesForTest(Class<?> testClazz) {
        var dir = testDir(testClazz);
        return subDirectories(dir);
    }

    private static @NonNull Path testDir(Class<?> testClazz) {
        return Path.of("target", "test-classes", testClazz.getSimpleName());
    }

    @NonNull
    static List<Path> subDirectories(Path dir) {
        var directories = new ArrayList<Path>();
        try (var expected = Files.newDirectoryStream(dir, Files::isDirectory)) {
            for (Path f : expected) {
                directories.add(f);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return directories;
    }

    static List<Path> recursiveFilesInDirectoryForTest(Class<?> testClazz, String filenameGlob) {
        Path path = testDir(testClazz);
        return recursiveFilesInDirectory(filenameGlob, path);
    }

    @VisibleForTesting
    static @NonNull List<Path> recursiveFilesInDirectory(String filenameGlob, Path path) {
        PathMatcher globMatcher = FileSystems.getDefault().getPathMatcher("glob:**/" + filenameGlob);
        List<Path> files = new ArrayList<>();
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    if (globMatcher.matches(file)) {
                        files.add(file);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return files;
    }
}