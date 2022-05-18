/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
