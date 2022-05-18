package com.github.tombentley.krpccodegen.maven;

import java.text.MessageFormat;
import java.util.ResourceBundle;

import org.apache.maven.plugin.logging.Log;

public class MavenLogger implements System.Logger {

    private final String name;
    private final Log mavenLog;

    public MavenLogger(String name, Log mavenLog) {
        this.name = name;
        this.mavenLog = mavenLog;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isLoggable(Level level) {
        switch (level) {
            case ALL:
                return true;
            case TRACE:
            case DEBUG:
                return mavenLog.isDebugEnabled();
            case INFO:
                return mavenLog.isInfoEnabled();
            case WARNING:
                return mavenLog.isWarnEnabled();
            case ERROR:
                return mavenLog.isErrorEnabled();
            case OFF:
            default:
                return false;
        }
    }

    @Override
    public void log(Level level, ResourceBundle bundle, String msg, Throwable thrown) {
        switch (level) {
            case TRACE:
            case DEBUG:
                mavenLog.debug(msg, thrown);
                break;
            case INFO:
                mavenLog.info(msg, thrown);
                break;
            case WARNING:
                mavenLog.warn(msg, thrown);
                break;
            case ALL:
            case ERROR:
                mavenLog.error(msg, thrown);
                break;
            case OFF:
            default:
                break;
        }
    }

    @Override
    public void log(Level level, ResourceBundle bundle, String format, Object... params) {
        switch (level) {
            case TRACE:
            case DEBUG:
                mavenLog.debug(new MessageFormat(format).format(params));
                break;
            case INFO:
                mavenLog.info(new MessageFormat(format).format(params));
                break;
            case WARNING:
                mavenLog.warn(new MessageFormat(format).format(params));
                break;
            case ALL:
            case ERROR:
                mavenLog.error(new MessageFormat(format).format(params));
                break;
            case OFF:
            default:
                break;
        }
    }
}
