/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.request;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.ProduceRequestData;

import io.kroxylicious.proxy.filter.schema.validation.topic.TopicValidationResult;
import io.kroxylicious.proxy.filter.schema.validation.topic.TopicValidator;
import io.kroxylicious.proxy.filter.schema.validation.topic.TopicValidators;

/**
 * A validator that can apply a different validation to each topic contained in
 * a single produce request. It routes each topic's data to a validator instance using
 * a list of rules. It applies the respective validator to each topic and then collects
 * and returns a result describing the Results for all topics.
 * <p>
 * If no rule is matched for a topic, then a (configurable) default validator will be
 * applied to the data for that topic.
 * </p>
 */
public class RoutingProduceRequestValidator implements ProduceRequestValidator {

    private final List<RoutingRule> rules;
    private final TopicValidator defaultValidator;
    private final Map<String, TopicValidator> cache = new HashMap<>();

    private record RoutingRule(Predicate<String> topicPredicate, TopicValidator validator) {

    }

    private RoutingProduceRequestValidator(List<RoutingRule> rules, TopicValidator defaultValidator) {
        if (rules == null) {
            throw new IllegalArgumentException("rules is null");
        }
        if (defaultValidator == null) {
            throw new IllegalArgumentException("defaultValidator is null");
        }
        this.rules = rules;
        this.defaultValidator = defaultValidator;
    }

    @Override
    public ProduceRequestValidationResult validateRequest(ProduceRequestData request) {
        Map<String, TopicValidationResult> collect = request.topicData().stream().collect(
                Collectors.toMap(ProduceRequestData.TopicProduceData::name, topicProduceData -> getTopicValidator(topicProduceData).validateTopicData(topicProduceData)));
        return new ProduceRequestValidationResult(collect);
    }

    private TopicValidator getTopicValidator(ProduceRequestData.TopicProduceData topicProduceData) {
        return cache.computeIfAbsent(topicProduceData.name(), topicName -> {
            Optional<RoutingRule> first = rules.stream().filter(routingRule -> routingRule.topicPredicate().test(topicName)).findFirst();
            return first.map(RoutingRule::validator).orElse(defaultValidator);
        });
    }

    /**
     * builder
     * @return builder
     */
    public static RoutingProduceRequestValidatorBuilder builder() {
        return new RoutingProduceRequestValidatorBuilder();
    }

    /**
     * Builder for RoutingProduceRequestValidator
     */
    public static class RoutingProduceRequestValidatorBuilder {
        private TopicValidator defaultValidator = TopicValidators.allValid();
        private final List<RoutingRule> routingRules = new ArrayList<>();

        private RoutingProduceRequestValidatorBuilder() {
        }

        /**
         * set the default validator to be used if no RoutingRule matches a topic
         * @param validator default validator
         * @return this RoutingProduceRequestValidatorBuilder
         */
        public RoutingProduceRequestValidatorBuilder setDefaultValidator(TopicValidator validator) {
            if (validator == null) {
                throw new IllegalArgumentException("attempted to set a null default validator");
            }
            this.defaultValidator = validator;
            return this;
        }

        /**
         * append a validator rule for a topic pattern (note order matters, rules are applied to topics in append order)
         * @param topicNames topic names
         * @param validator validator to be applied if pattern matches topic
         * @return this RoutingProduceRequestValidatorBuilder
         */
        public RoutingProduceRequestValidatorBuilder appendValidatorForTopicPattern(Set<String> topicNames, TopicValidator validator) {
            routingRules.add(new RoutingRule(topicNames::contains, validator));
            return this;
        }

        /**
         * build the routing ProduceRequestValidator
         * @return validator
         */
        public ProduceRequestValidator build() {
            return new RoutingProduceRequestValidator(routingRules, defaultValidator);
        }

    }
}
