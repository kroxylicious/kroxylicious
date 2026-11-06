/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

import org.slf4j.LoggerFactory;

/**
 * Provides the Kroxylicious version and git commit id, as recorded in an
 * artifact's version metadata resource at build time.
 */
public interface VersionInfo {

    /**
     * Version information loaded from the default Kroxylicious runtime metadata
     * resource, {@code META-INF/metadata.properties}.
     *
     * <p>
     * Other Kroxylicious artifacts should use {@link #fromResource(String)}
     * when their version metadata is stored in an artifact-specific resource.
     */
    VersionInfo VERSION_INFO = getVersionInfo();

    /**
     * Returns the Kroxylicious version, or {@code unknown} if it could not be determined.
     *
     * @return the version
     */
    String version();

    /**
     * Returns the git commit id the artifact was built from, or {@code unknown} if it could
     * not be determined.
     *
     * @return the commit id
     */
    String commitId();

    /**
     * Loads version information from the supplied classpath resource.
     *
     * @param resourceName the classpath resource containing version metadata
     * @return the version information, or unknown values if the resource cannot be
     *         loaded
     */
    static VersionInfo fromResource(String resourceName) {
        return getVersionInfo(resourceName);
    }

    private static VersionInfo getVersionInfo() {
        return fromResource("META-INF/metadata.properties");
    }

    private static VersionInfo getVersionInfo(String resourceName) {

        try (var resource = DefaultVersionInfo.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (resource != null) {
                Properties properties = new Properties();
                properties.load(resource);
                String version = properties.getProperty("kroxylicious.version", DefaultVersionInfo.UNKNOWN);
                String commitId = properties.getProperty("git.commit.id", DefaultVersionInfo.UNKNOWN);
                return new DefaultVersionInfo(version, commitId);
            }
        }
        catch (IOException e) {
            var logger = LoggerFactory.getLogger(DefaultVersionInfo.class);
            logger.atWarn()
                    .setCause(e)
                    .log("Failed to retrieve version information (ignored)");
        }
        return DefaultVersionInfo.UNKNOWN_VERSION_INFO;
    }

    /**
     * Default {@link VersionInfo} implementation backed by values read from a version metadata
     * properties resource, falling back to {@code unknown} values when unavailable.
     */
    final class DefaultVersionInfo implements VersionInfo {
        private static final String UNKNOWN = "unknown";

        private static final DefaultVersionInfo UNKNOWN_VERSION_INFO = new DefaultVersionInfo(UNKNOWN, UNKNOWN);
        private final String version;
        private final String commitId;

        private DefaultVersionInfo(String version, String commitId) {
            this.version = Objects.requireNonNull(version);
            this.commitId = Objects.requireNonNull(commitId);
        }

        @Override
        public String version() {
            return version;
        }

        @Override
        public String commitId() {
            return commitId;
        }

    }
}
