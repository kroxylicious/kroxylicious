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
    VersionInfo VERSION_INFO = getVersionInfo();

    String version();

    String commitId();

    private static VersionInfo getVersionInfo() {

        try (var resource = Info.class.getClassLoader().getResourceAsStream("META-INF/metadata.properties")) {
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
            logger.warn("Failed to retrieve version information (ignored)", e);
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
