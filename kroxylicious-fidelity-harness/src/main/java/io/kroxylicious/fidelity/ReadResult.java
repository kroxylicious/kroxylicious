/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity;

/**
 * The result of decoding a message from bytes: the populated instance, and how many bytes of the input
 * were left unconsumed. A correct decode consumes every byte. If decoding threw, {@code unreadBytes} is
 * however many bytes remained unconsumed at the point of failure, and {@code error} holds the exception
 * rather than propagating it.
 *
 * @param message the populated instance
 * @param unreadBytes the number of bytes remaining in the input after decoding
 * @param error the exception thrown while decoding, or {@code null} if decoding succeeded
 * @param <T> the message type
 */
public record ReadResult<T>(T message, int unreadBytes, Throwable error) {}