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

    String version();

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

        try (var resource = Info.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (resource != null) {
                Properties properties = new Properties();
                properties.load(resource);
                String version = properties.getProperty("kroxylicious.version", Info.UNKNOWN);
                String commitId = properties.getProperty("git.commit.id", Info.UNKNOWN);
                return new Info(version, commitId);
            }
        }
        catch (IOException e) {
            var logger = LoggerFactory.getLogger(Info.class);
            logger.atWarn()
                    .setCause(e)
                    .log("Failed to retrieve version information (ignored)");
        }
        return Info.UNKNOWN_VERSION_INFO;
    }

    final class Info implements VersionInfo {
        private static final String UNKNOWN = "unknown";

        private static final Info UNKNOWN_VERSION_INFO = new Info(UNKNOWN, UNKNOWN);
        private final String version;
        private final String commitId;

        private Info(String version, String commitId) {
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
