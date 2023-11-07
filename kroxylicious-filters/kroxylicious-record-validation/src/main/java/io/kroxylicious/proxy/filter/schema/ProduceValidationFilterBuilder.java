/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema;

import io.kroxylicious.proxy.filter.schema.config.BytebufValidation;
import io.kroxylicious.proxy.filter.schema.config.RecordValidationRule;
import io.kroxylicious.proxy.filter.schema.config.ValidationConfig;
import io.kroxylicious.proxy.filter.schema.validation.bytebuf.BytebufValidator;
import io.kroxylicious.proxy.filter.schema.validation.bytebuf.BytebufValidators;
import io.kroxylicious.proxy.filter.schema.validation.record.KeyAndValueRecordValidator;
import io.kroxylicious.proxy.filter.schema.validation.request.ProduceRequestValidator;
import io.kroxylicious.proxy.filter.schema.validation.request.RoutingProduceRequestValidator;
import io.kroxylicious.proxy.filter.schema.validation.topic.TopicValidator;
import io.kroxylicious.proxy.filter.schema.validation.topic.TopicValidators;

/**
 * Builds from configuration objects to a ProduceRequestValidator
 */
public class ProduceValidationFilterBuilder {

    private ProduceValidationFilterBuilder() {

    }

    /**
     * Build a ProduceRequestValidator from configuration
     * @param config configuration
     * @return a ProduceRequestValidator
     */
    public static ProduceRequestValidator build(ValidationConfig config) {
        RoutingProduceRequestValidator.RoutingProduceRequestValidatorBuilder builder = RoutingProduceRequestValidator.builder();
        config.getRules().forEach(rule -> builder.appendValidatorForTopicPattern(rule.getTopicNames(), toValidatorWithNullHandling(rule)));
        RecordValidationRule defaultRule = config.getDefaultRule();
        TopicValidator defaultValidator = defaultRule == null ? TopicValidators.allValid() : toValidatorWithNullHandling(defaultRule);
        builder.setDefaultValidator(defaultValidator);
        return builder.build();
    }

    private static TopicValidator toValidatorWithNullHandling(RecordValidationRule validationRule) {
        BytebufValidator keyValidator = validationRule.getKeyRule().map(ProduceValidationFilterBuilder::getBytebufValidator).orElse(BytebufValidators.allValid());
        BytebufValidator valueValidator = validationRule.getValueRule().map(ProduceValidationFilterBuilder::getBytebufValidator).orElse(BytebufValidators.allValid());
        return TopicValidators.perRecordValidator(KeyAndValueRecordValidator.keyAndValueValidator(keyValidator, valueValidator));
    }

    private static BytebufValidator getBytebufValidator(BytebufValidation validation) {
        BytebufValidator innerValidator = toValidator(validation);
        return BytebufValidators.nullEmptyValidator(validation.isAllowNulls(), validation.isAllowEmpty(), innerValidator);
    }

    private static BytebufValidator toValidator(BytebufValidation valueRule) {
        return valueRule.getSyntacticallyCorrectJsonConfig().map(config -> BytebufValidators.jsonSyntaxValidator(config.isValidateObjectKeysUnique()))
                .orElse(BytebufValidators.allValid());
    }

}
