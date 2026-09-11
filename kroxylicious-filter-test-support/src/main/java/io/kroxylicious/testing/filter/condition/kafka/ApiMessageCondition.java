/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter.condition.kafka;

import java.util.function.Predicate;

import org.assertj.core.api.Condition;
import org.assertj.core.description.Description;
import org.assertj.core.description.TextDescription;

import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

/**
 * An AssertJ {@link Condition} that matches {@link ApiMessage} instances satisfying a predicate.
 *
 * @param <X> the type of ApiMessage the condition applies to
 */
public class ApiMessageCondition<X extends ApiMessage> extends Condition<X> {

    private final Predicate<X> predicate;

    /**
     * Constructs a condition matching ApiMessages that satisfy the given predicate.
     *
     * @param xPredicate the predicate the message must satisfy
     * @param description the description of the condition used in failure messages
     */
    public ApiMessageCondition(Predicate<X> xPredicate, Description description) {
        super(description);
        this.predicate = xPredicate;
    }

    /**
     * Creates a condition matching ApiMessages with the given API key.
     *
     * @param <X> the type of ApiMessage the condition applies to
     * @param expectedApiKey the expected API key
     * @return the condition
     */
    public static <X extends ApiMessage> ApiMessageCondition<X> forApiKey(short expectedApiKey) {
        return new ApiMessageCondition<>(apiMessage -> apiMessage.apiKey() == expectedApiKey,
                new TextDescription("an ApiMessage of type %s (%d)", ApiKeys.forId(expectedApiKey), expectedApiKey));
    }

    /**
     * Creates a condition matching ApiMessages with the given API key.
     *
     * @param <X> the type of ApiMessage the condition applies to
     * @param expectedApiKey the expected API key
     * @return the condition
     */
    public static <X extends ApiMessage> ApiMessageCondition<X> forApiKey(ApiKeys expectedApiKey) {
        return new ApiMessageCondition<>(apiMessage -> ApiKeys.forId(apiMessage.apiKey()) == expectedApiKey,
                new TextDescription("an ApiMessage of type %s (%d)", expectedApiKey, expectedApiKey.id));
    }

    /**
     * Constructs a condition matching ApiMessages that satisfy the given predicate.
     *
     * @param predicate the predicate the message must satisfy
     */
    public ApiMessageCondition(Predicate<X> predicate) {
        this(predicate, new TextDescription("an Api Message matching a custom predicate"));
    }

    @Override
    public boolean matches(X apiMessage) {
        if (apiMessage != null) {
            return predicate.test(apiMessage);
        }
        else {
            return false;
        }
    }
}
