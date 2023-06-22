/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Assertions;

import io.netty.util.ResourceLeakDetector;

@Plugin(name = "NettyLeakLogAppender", category = "Core", elementType = "appender", printObject = true)
public class NettyLeakLogAppender extends AbstractAppender {
    private List<String> leaks = new ArrayList<>();

    public void verifyNoLeaks() {
        if (!leaks.isEmpty()) {
            Assertions.fail("Netty resource leak detected " + leaks);
        }
    }

    protected NettyLeakLogAppender(String name, Filter filter, Layout<? extends Serializable> layout) {
        super(name, filter, layout, true, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
        if (event.getLoggerName().equals(ResourceLeakDetector.class.getName()) && event.getLevel().equals(Level.ERROR) && event.getMessage().getFormattedMessage()
                .contains("LEAK:")) {
            leaks.add(event.getMessage().getFormattedMessage());
        }
    }

    @PluginFactory
    public static NettyLeakLogAppender createAppender(
                                                      @PluginAttribute("name") String name,
                                                      @PluginElement("Layout") Layout<? extends Serializable> layout,
                                                      @PluginElement("Filter") final Filter filter,
                                                      @PluginAttribute("otherAttribute") String otherAttribute) {
        if (name == null) {
            LOGGER.error("No name provided for TestAppender");
            return null;
        }
        if (layout == null) {
            layout = PatternLayout.createDefaultLayout();
        }
        return new NettyLeakLogAppender(name, filter, layout);
    }

    public void clear() {
        leaks.clear();
    }
}
