/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation;

import java.util.ArrayList;
import java.util.Map;

import io.apicurio.registry.resolver.config.SchemaResolverConfig;

import io.kroxylicious.proxy.filter.validation.config.BytebufValidation;
import io.kroxylicious.proxy.filter.validation.config.RecordValidationRule;
import io.kroxylicious.proxy.filter.validation.config.ValidationConfig;
import io.kroxylicious.proxy.filter.validation.validators.bytebuf.BytebufValidator;
import io.kroxylicious.proxy.filter.validation.validators.bytebuf.BytebufValidators;
import io.kroxylicious.proxy.filter.validation.validators.record.KeyAndValueRecordValidator;
import io.kroxylicious.proxy.filter.validation.validators.request.ProduceRequestValidator;
import io.kroxylicious.proxy.filter.validation.validators.request.RoutingProduceRequestValidator;
import io.kroxylicious.proxy.filter.validation.validators.topic.TopicValidator;
import io.kroxylicious.proxy.filter.validation.validators.topic.TopicValidators;

/**
 * Builds from configuration objects to a ProduceRequestValidator
 */
class ProduceRequestValidatorBuilder {

    private ProduceRequestValidatorBuilder() {

    }

    /**
     * Build a ProduceRequestValidator from configuration
     * @param config configuration
     * @return a ProduceRequestValidator
     */
    static ProduceRequestValidator build(ValidationConfig config) {
        RoutingProduceRequestValidator.RoutingProduceRequestValidatorBuilder builder = RoutingProduceRequestValidator.builder();
        config.rules().forEach(rule -> builder.appendValidatorForTopicPattern(rule.getTopicNames(), toValidatorWithNullHandling(rule)));
        RecordValidationRule defaultRule = config.defaultRule();
        TopicValidator defaultValidator = defaultRule == null ? TopicValidators.allValid() : toValidatorWithNullHandling(defaultRule);
        builder.setDefaultValidator(defaultValidator);
        return builder.build();
    }

    private static TopicValidator toValidatorWithNullHandling(RecordValidationRule validationRule) {
        BytebufValidator keyValidator = validationRule.getKeyRule().map(ProduceRequestValidatorBuilder::getBytebufValidator).orElse(BytebufValidators.allValid());
        BytebufValidator valueValidator = validationRule.getValueRule().map(ProduceRequestValidatorBuilder::getBytebufValidator).orElse(BytebufValidators.allValid());
        return TopicValidators.perRecordValidator(KeyAndValueRecordValidator.keyAndValueValidator(keyValidator, valueValidator));
    }

    private static BytebufValidator getBytebufValidator(BytebufValidation validation) {
        BytebufValidator innerValidator = toValidator(validation);
        return BytebufValidators.nullEmptyValidator(validation.isAllowNulls(), validation.isAllowEmpty(), innerValidator);
    }

    private static BytebufValidator toValidator(BytebufValidation valueRule) {
        var validators = new ArrayList<BytebufValidator>();
        valueRule.getSyntacticallyCorrectJsonConfig().ifPresent(config -> validators.add(BytebufValidators.jsonSyntaxValidator(config.isValidateObjectKeysUnique())));
        valueRule.getSchemaValidationConfig().ifPresent(
                config -> validators.add(BytebufValidators.jsonSchemaValidator(Map.of(SchemaResolverConfig.REGISTRY_URL, config.apicurioRegistryUrl().toString()),
                        config.apicurioContentId())));

        return BytebufValidators.chainOf(validators);
    }

}
