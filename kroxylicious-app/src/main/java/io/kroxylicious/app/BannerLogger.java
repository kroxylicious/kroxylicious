/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.event.Level;

import io.kroxylicious.proxy.tag.VisibleForTesting;

import static org.slf4j.LoggerFactory.getLogger;

public class BannerLogger {

    private static final String DEFAULT_BANNER_LOCATION = "banner.txt";
    private final Logger targetLogger;
    private final Supplier<Stream<String>> bannerReader;
    private final Level targetLevel;

    public BannerLogger() {
        this(
                getLogger("io.kroxylicious.proxy.StartupShutdownLogger"),
                new BannerSupplier(DEFAULT_BANNER_LOCATION)
        );
    }

    BannerLogger(Logger targetLogger, Supplier<Stream<String>> bannerReader) {
        this.targetLogger = targetLogger;
        this.bannerReader = bannerReader;
        targetLevel = Level.INFO;
    }

    public void log() {
        bannerReader.get().forEach(line -> targetLogger.atLevel(targetLevel).log(line));
    }

    @VisibleForTesting
    static class BannerSupplier implements Supplier<Stream<String>> {

        private static final String LICENSE_TXT = "license.txt";
        private final Supplier<Stream<String>> linesSupplier;
        private final Set<String> licenseLines;

        BannerSupplier(String resourceName) {
            this(new FileLinesSupplier(resourceName), new FileLinesSupplier(LICENSE_TXT));
        }

        BannerSupplier(Supplier<Stream<String>> bannerLinesSupplier, Supplier<Stream<String>> licenceStream) {
            linesSupplier = bannerLinesSupplier;
            licenseLines = licenceStream.get().collect(Collectors.toSet());
        }

        @Override
        public Stream<String> get() {
            return linesSupplier.get().filter(Predicate.not(licenseLines::contains));
        }
    }

    @VisibleForTesting
    static class FileLinesSupplier implements Supplier<Stream<String>> {

        private final String resourceName;

        FileLinesSupplier(String resourceName) {
            this.resourceName = resourceName;
        }

        @Override
        public Stream<String> get() {
            var resourceStream = getClass().getClassLoader().getResourceAsStream(resourceName);
            if (Objects.isNull(resourceStream)) {
                return Stream.empty();
            } else {
                var bufferedReader = new BufferedReader(new InputStreamReader(resourceStream, Charset.defaultCharset()));
                return bufferedReader.lines().onClose(() -> {
                    try {
                        resourceStream.close();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
        }
    }
}
