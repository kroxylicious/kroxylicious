/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

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
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public class BannerLogger {

    public static final String DEFAULT_BANNER_LOCATION = "banner.txt";
    private final Logger targetLogger;
    private final Supplier<Stream<String>> bannerReader;
    private final Level targetLevel;

    public BannerLogger() {
        this(LoggerFactory.getLogger("io.kroxylicious.proxy.internal.StartupShutdownLogger"),
                new BannerSupplier(DEFAULT_BANNER_LOCATION));
    }

    BannerLogger(Logger targetLogger, Supplier<Stream<String>> bannerReader) {
        this.targetLogger = targetLogger;
        this.bannerReader = bannerReader;
        targetLevel = Level.INFO; // TODO should this be configurable?¡
    }

    public void log() {
        bannerReader.get().forEach(line -> targetLogger.atLevel(targetLevel).log(line));
    }

    /* test */ static class BannerSupplier implements Supplier<Stream<String>> {

        private final Supplier<Stream<String>> linesSupplier;
        private final Set<String> licenseLines;

        BannerSupplier(String resourceName) {
            this(new FileLinesSupplier(resourceName), new FileLinesSupplier("etc/LICENSE.txt"));
        }

        BannerSupplier(Supplier<Stream<String>> bannerLinesSupplier, Supplier<Stream<String>> licenceStream) {
            linesSupplier = bannerLinesSupplier;
            licenseLines = Stream.concat(Stream.of("===="), licenceStream.get()).collect(Collectors.toSet());
        }

        @Override
        public Stream<String> get() {
            return linesSupplier.get().filter(Predicate.not(licenseLines::contains));
        }
    }

    /* test */ static class FileLinesSupplier implements Supplier<Stream<String>> {

        private final String resourceName;

        FileLinesSupplier(String resourceName) {
            this.resourceName = resourceName;
        }

        @Override
        public Stream<String> get() {
            try (var resourceStream = getClass().getClassLoader().getResourceAsStream(resourceName)) {
                if (Objects.isNull(resourceStream)) {
                    return Stream.empty();
                }
                else {
                    final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(resourceStream, Charset.defaultCharset()));
                    return bufferedReader.lines();
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
