/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.audit.emitter.slf4j;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.audit.AuditEmitter;
import io.kroxylicious.proxy.audit.AuditableAction;

import edu.umd.cs.findbugs.annotations.Nullable;

class Slf4jEmitter implements AuditEmitter {

    private final Map<ActionMatch, Level> levels;

    private final Level defaultLevel;

    Slf4jEmitter(Level defaultLevel, Map<ActionMatch, Level> levels) {
        this.defaultLevel = defaultLevel;
        this.levels = Map.copyOf(levels); // copy so that it cannot be modified
    }

    private Level levelForAction(String action, boolean success) {
        return levels.getOrDefault(new ActionMatch(action, success), defaultLevel);
    }

    @Override
    public boolean isInterested(String action, @Nullable String status) {
        // Get the logger for the action
        final Logger logger = loggerForAction(action);
        // What level does the config say we should report this action, status combination as?
        return switch (levelForAction(action, status == null)) {
            // We're only interested if the logger for the action is enabled for the level we're configured to log it at
            case OFF -> false;
            case TRACE -> logger.isTraceEnabled();
            case DEBUG -> logger.isDebugEnabled();
            case INFO -> logger.isInfoEnabled();
            case WARN -> logger.isWarnEnabled();
            case ERROR -> logger.isErrorEnabled();
        };
    }

    @Override
    public void emitAction(AuditableAction action, Context context) {
        final Logger logger = loggerForAction(action.action());
        var builder = switch (levelForAction(action.action(), action.status() == null)) {
            case OFF -> null;
            case TRACE -> logger.atTrace();
            case DEBUG -> logger.atDebug();
            case INFO -> logger.atInfo();
            case WARN -> logger.atWarn();
            case ERROR -> logger.atError();
        };
        if (builder != null) {
            builder.setMessage(context.asString(action, TextFormat.KROXYLICIOUS_JSON_V1))
                    .log();
        }
    }

    private Logger loggerForAction(String action) {
        return LoggerFactory.getLogger("audit." + action);
    }

    @Override
    public void close() {
        // We're not responsible for any resources which need to be closed.
    }
}
