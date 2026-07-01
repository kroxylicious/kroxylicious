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
    VersionInfo VERSION_INFO = fromResource("META-INF/metadata.properties");

    String version();

    String commitId();

    /**
     * Loads version information from the named classpath properties resource, reading the
     * {@code kroxylicious.version} and {@code git.commit.id} properties. Each module that wants to
     * report its own build metadata should ship a distinctly named resource and load it here, so
     * that the reported version reflects the module's own build rather than whichever
     * {@code metadata.properties} happens to be first on the classpath.
     *
     * @param resourceName the classpath resource to load the metadata from
     * @return the version information, or unknown values if the resource is absent or unreadable
     */
    static VersionInfo fromResource(String resourceName) {

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
