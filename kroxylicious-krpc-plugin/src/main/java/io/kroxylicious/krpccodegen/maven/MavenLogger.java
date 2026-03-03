/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.maven;

import java.text.MessageFormat;
import java.util.ResourceBundle;

import org.apache.maven.plugin.logging.Log;

class MavenLogger implements System.Logger {

    private final String name;
    private final Log mavenLog;

    MavenLogger(String name, Log mavenLog) {
        this.name = name;
        this.mavenLog = mavenLog;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isLoggable(Level level) {
        return switch (level) {
            case ALL -> true;
            case TRACE, DEBUG -> mavenLog.isDebugEnabled();
            case INFO -> mavenLog.isInfoEnabled();
            case WARNING -> mavenLog.isWarnEnabled();
            case ERROR -> mavenLog.isErrorEnabled();
            default -> false;
        };
    }

    @Override
    public void log(Level level, ResourceBundle bundle, String msg, Throwable thrown) {
        switch (level) {
            case TRACE, DEBUG -> mavenLog.debug(msg, thrown);
            case INFO -> mavenLog.info(msg, thrown);
            case WARNING -> mavenLog.warn(msg, thrown);
            case ALL, ERROR -> mavenLog.error(msg, thrown);
            default -> {
                // log nothing
            }
        }
    }

    @Override
    public void log(Level level, ResourceBundle bundle, String format, Object... params) {
        switch (level) {
            case TRACE, DEBUG -> mavenLog.debug(new MessageFormat(format).format(params));
            case INFO -> mavenLog.info(new MessageFormat(format).format(params));
            case WARNING -> mavenLog.warn(new MessageFormat(format).format(params));
            case ALL, ERROR -> mavenLog.error(new MessageFormat(format).format(params));
            default -> {
                // log nothing
            }
        }
    }
}
