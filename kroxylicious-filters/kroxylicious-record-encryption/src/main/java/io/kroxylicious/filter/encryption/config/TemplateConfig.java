/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

/**
 * The configuration for the {@link io.kroxylicious.filter.encryption.TemplateKekSelector} plugin.
 * @param template The template used to derive the KEK alias from the topic name. The {@code $(topicName)}
 *        placeholder is replaced with the name of the topic.
 */
public record TemplateConfig(String template) {}
