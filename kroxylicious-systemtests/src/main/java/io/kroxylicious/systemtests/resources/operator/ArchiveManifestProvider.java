/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.operator;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.List;
import java.util.function.Predicate;

import io.kroxylicious.systemtests.Constants;

/**
 * Provides manifests from an extracted archive directory.
 * Supports the traditional approach where manifests are extracted from ZIP/TAR archives.
 */
public class ArchiveManifestProvider implements ManifestProvider {

    private final Path installDir;

    public ArchiveManifestProvider(Path installDir) {
        this.installDir = installDir;
    }

    @Override
    public List<File> getCrdYamls() {
        return installFilesMatching(glob(Constants.OPERATOR_INSTALL_CRD_GLOB));
    }

    @Override
    public List<File> getInstallYamls() {
        return installFilesMatching(
                Predicate.not(glob(Constants.OPERATOR_INSTALL_CRD_GLOB)));
    }

    private List<File> installFilesMatching(Predicate<Path> matcher) {
        List<File> files;
        try (var fileStream = Files.list(installDir)) {
            files = fileStream.filter(Files::isRegularFile)
                    .filter(matcher)
                    .sorted()
                    .map(Path::toFile)
                    .toList();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return files;
    }

    private Predicate<Path> glob(String glob) {
        PathMatcher pathMatcher = FileSystems.getDefault()
                .getPathMatcher("glob:" + glob);
        return path -> pathMatcher.matches(path.getFileName());
    }

    private List<File> getFilteredOperatorFiles(Predicate<Path> predicate) {
        return installFilesMatching(predicate);
    }
}
