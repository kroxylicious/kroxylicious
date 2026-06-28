/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.maven;

import java.lang.System.Logger.Level;
import java.util.ResourceBundle;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.maven.plugin.logging.Log;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MavenLoggerTest {

    public static final String MESSAGE = "message";
    @Mock
    private Log logger;

    @Mock
    private ResourceBundle resourceBundle;

    static Stream<Arguments> isLoggable() {
        return Stream.of(argumentSet("ALL is always loggable", Level.ALL, mockLogger(log -> {
        }), true),
                argumentSet("DEBUG loggable when delegate debug enabled", Level.DEBUG, mockLogger(log -> {
                    when(log.isDebugEnabled()).thenReturn(true);
                }), true),
                argumentSet("DEBUG not loggable when delegate debug disabled", Level.DEBUG, mockLogger(log -> {
                    when(log.isDebugEnabled()).thenReturn(false);
                }), false),
                argumentSet("TRACE loggable when delegate debug enabled", Level.TRACE, mockLogger(log -> {
                    when(log.isDebugEnabled()).thenReturn(true);
                }), true),
                argumentSet("TRACE not loggable when delegate debug disabled", Level.TRACE, mockLogger(log -> {
                    when(log.isDebugEnabled()).thenReturn(false);
                }), false),
                argumentSet("INFO loggable when delegate info enabled", Level.INFO, mockLogger(log -> {
                    when(log.isInfoEnabled()).thenReturn(true);
                }), true),
                argumentSet("INFO not loggable when delegate info disabled", Level.INFO, mockLogger(log -> {
                    when(log.isInfoEnabled()).thenReturn(false);
                }), false),
                argumentSet("WARN loggable when delegate warn enabled", Level.WARNING, mockLogger(log -> {
                    when(log.isWarnEnabled()).thenReturn(true);
                }), true),
                argumentSet("WARN not loggable when delegate warn disabled", Level.WARNING, mockLogger(log -> {
                    when(log.isWarnEnabled()).thenReturn(false);
                }), false),
                argumentSet("ERROR loggable when delegate error enabled", Level.ERROR, mockLogger(log -> {
                    when(log.isErrorEnabled()).thenReturn(true);
                }), true),
                argumentSet("ERROR not loggable when delegate error disabled", Level.ERROR, mockLogger(log -> {
                    when(log.isErrorEnabled()).thenReturn(false);
                }), false));
    }

    private static Supplier<Log> mockLogger(Consumer<Log> withConfig) {
        return () -> {
            Log log = mock(Log.class);
            withConfig.accept(log);
            return log;
        };
    }

    @ParameterizedTest
    @MethodSource
    void isLoggable(Level level,
                    Supplier<Log> prepareDelegate,
                    boolean expectedIsLoggable) {
        Log delegate = prepareDelegate.get();
        MavenLogger mavenLogger = new MavenLogger("logger", delegate);
        boolean loggable = mavenLogger.isLoggable(level);
        assertThat(loggable).isEqualTo(expectedIsLoggable);
    }

    @Test
    void logTraceWithThrowable() {
        MavenLogger mavenLogger = new MavenLogger("logger", logger);
        Throwable error = new Throwable();
        mavenLogger.log(Level.TRACE, resourceBundle, MESSAGE, error);
        verify(logger).debug(MESSAGE, error);
    }

    @Test
    void logDebugWithThrowable() {
        MavenLogger mavenLogger = new MavenLogger("logger", logger);
        Throwable error = new Throwable();
        mavenLogger.log(Level.DEBUG, resourceBundle, MESSAGE, error);
        verify(logger).debug(MESSAGE, error);
    }

    @Test
    void logInfoWithThrowable() {
        MavenLogger mavenLogger = new MavenLogger("logger", logger);
        Throwable error = new Throwable();
        mavenLogger.log(Level.INFO, resourceBundle, MESSAGE, error);
        verify(logger).info(MESSAGE, error);
    }

    @Test
    void logWarnWithThrowable() {
        MavenLogger mavenLogger = new MavenLogger("logger", logger);
        Throwable error = new Throwable();
        mavenLogger.log(Level.WARNING, resourceBundle, MESSAGE, error);
        verify(logger).warn(MESSAGE, error);
    }

    @Test
    void logErrorWithThrowable() {
        MavenLogger mavenLogger = new MavenLogger("logger", logger);
        Throwable error = new Throwable();
        mavenLogger.log(Level.ERROR, resourceBundle, MESSAGE, error);
        verify(logger).error(MESSAGE, error);
    }

    @Test
    void logAllWithThrowable() {
        MavenLogger mavenLogger = new MavenLogger("logger", logger);
        Throwable error = new Throwable();
        mavenLogger.log(Level.ALL, resourceBundle, MESSAGE, error);
        verify(logger).error(MESSAGE, error);
    }

    @Test
    void logTraceWithFormatAndParams() {
        MavenLogger mavenLogger = new MavenLogger("logger", logger);
        mavenLogger.log(Level.TRACE, resourceBundle, "{0},{1}", "param1", "param2");
        verify(logger).debug("param1,param2");
    }

    @Test
    void logDebugWithFormatAndParams() {
        MavenLogger mavenLogger = new MavenLogger("logger", logger);
        mavenLogger.log(Level.DEBUG, resourceBundle, "{0},{1}", "param1", "param2");
        verify(logger).debug("param1,param2");
    }

    @Test
    void logInfoWithFormatAndParams() {
        MavenLogger mavenLogger = new MavenLogger("logger", logger);
        mavenLogger.log(Level.INFO, resourceBundle, "{0},{1}", "param1", "param2");
        verify(logger).info("param1,param2");
    }

    @Test
    void logWarnWithFormatAndParams() {
        MavenLogger mavenLogger = new MavenLogger("logger", logger);
        mavenLogger.log(Level.WARNING, resourceBundle, "{0},{1}", "param1", "param2");
        verify(logger).warn("param1,param2");
    }

    @Test
    void logErrorWithFormatAndParams() {
        MavenLogger mavenLogger = new MavenLogger("logger", logger);
        mavenLogger.log(Level.ERROR, resourceBundle, "{0},{1}", "param1", "param2");
        verify(logger).error("param1,param2");
    }

    @Test
    void logAllWithFormatAndParams() {
        MavenLogger mavenLogger = new MavenLogger("logger", logger);
        mavenLogger.log(Level.ALL, resourceBundle, "{0},{1}", "param1", "param2");
        verify(logger).error("param1,param2");
    }

}