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

import edu.umd.cs.findbugs.annotations.NonNull;

public interface VersionInfo {
    VersionInfo VERSION_INFO = getVersionInfo();

    String version();

    String commitId();

    String commitMessage();

    private static VersionInfo getVersionInfo() {

        try (var resource = Info.class.getClassLoader().getResourceAsStream("META-INF/metadata.properties")) {
            if (resource != null) {
                Properties properties = new Properties();
                properties.load(resource);
                String version = properties.getProperty("kroxylicious.version", Info.UNKNOWN);
                String commitId = properties.getProperty("git.commit.id", Info.UNKNOWN);
                String commitMessage = properties.getProperty("git.commit.message.short", Info.UNKNOWN);
                return new Info(version, commitId, commitMessage);
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

        private static final Info UNKNOWN_VERSION_INFO = new Info(UNKNOWN, UNKNOWN, UNKNOWN);
        private final String version;
        private final String commitId;
        private final String commitMessage;

        private Info(@NonNull String version, @NonNull String commitId, @NonNull String commitMessage) {
            this.version = Objects.requireNonNull(version);
            this.commitId = Objects.requireNonNull(commitId);
            this.commitMessage = Objects.requireNonNull(commitMessage);
        }

        @Override
        public String version() {
            return version;
        }

        @Override
        public String commitId() {
            return commitId;
        }

        @Override
        public String commitMessage() {
            return commitMessage;
        }

    }
}
