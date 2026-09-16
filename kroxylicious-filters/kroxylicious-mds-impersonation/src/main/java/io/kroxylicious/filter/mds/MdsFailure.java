/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.io.IOException;
import java.net.http.HttpTimeoutException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLException;

/** Diagnostics containing only local reason codes, numeric codes and sanitized stack frames. */
final class MdsFailure extends IllegalStateException {
    enum Reason {
        TOKEN_INVALID,
        TOKEN_SUBJECT,
        TOKEN_LIFETIME,
        RESPONSE_SIZE,
        MDS_HTTP,
        MDS_TIMEOUT,
        MDS_TLS,
        MDS_IO,
        MDS_CAPACITY,
        MDS_BACKOFF,
        MDS_CLOSED,
        REQUEST_ENCODING,
        UPSTREAM_VERSIONS,
        UPSTREAM_HANDSHAKE,
        UPSTREAM_AUTHENTICATE,
        SESSION_LIFETIME,
        IDENTITY,
        DOWNSTREAM_SASL,
        SESSION_EXPIRED,
        UNEXPECTED
    }

    private final Reason reason;
    private final int code;
    private final String errorType;

    MdsFailure(Reason reason) {
        this(reason, 0);
    }

    MdsFailure(Reason reason, int code) {
        this(reason, code, MdsFailure.class.getName());
    }

    private MdsFailure(Reason reason, int code, String errorType) {
        super("MDS authentication failed: " + reason + " (code=" + code + ")");
        this.reason = reason;
        this.code = code;
        this.errorType = errorType;
    }

    Reason reason() {
        return reason;
    }

    int code() {
        return code;
    }

    String errorType() {
        return errorType;
    }

    boolean serviceFailure() {
        return switch (reason) {
            case MDS_TIMEOUT, MDS_TLS, MDS_IO, MDS_CAPACITY -> true;
            case MDS_HTTP -> code == 429 || code >= 500;
            default -> false;
        };
    }

    static MdsFailure safe(Throwable error) {
        Throwable unwrapped = unwrap(error);
        if (unwrapped instanceof MdsFailure known) {
            return known;
        }
        Reason reason = switch (unwrapped) {
            case HttpTimeoutException ignored -> Reason.MDS_TIMEOUT;
            case TimeoutException ignored -> Reason.MDS_TIMEOUT;
            case SSLException ignored -> Reason.MDS_TLS;
            case IOException ignored -> Reason.MDS_IO;
            case RejectedExecutionException ignored -> Reason.MDS_CAPACITY;
            default -> Reason.UNEXPECTED;
        };
        return sanitized(reason, unwrapped);
    }

    static MdsFailure atStage(Reason stage, Throwable error) {
        Throwable unwrapped = unwrap(error);
        return unwrapped instanceof MdsFailure known ? known : sanitized(stage, unwrapped);
    }

    private static Throwable unwrap(Throwable error) {
        Throwable unwrapped = error;
        for (int depth = 0; depth < 16 && (unwrapped instanceof CompletionException || unwrapped instanceof ExecutionException)
                && unwrapped.getCause() != null; depth++) {
            unwrapped = unwrapped.getCause();
        }
        return unwrapped;
    }

    static MdsFailure sanitized(Reason reason, Throwable error) {
        var safe = new MdsFailure(reason, 0, error.getClass().getName());
        // HTTP parser errors can contain received data, not just JSON parser errors.
        // Retain the failure location, but never external messages, causes or suppressed errors.
        safe.setStackTrace(error.getStackTrace());
        return safe;
    }
}
