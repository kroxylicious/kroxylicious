/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.io.ByteArrayOutputStream;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import edu.umd.cs.findbugs.annotations.Nullable;

/** Bounds response buffering even when the peer omits Content-Length. */
final class LimitedBodySubscriber implements HttpResponse.BodySubscriber<byte[]> {
    private final int limit;
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final CompletableFuture<byte[]> result = new CompletableFuture<>();
    @Nullable
    private Flow.Subscription subscription;

    LimitedBodySubscriber(int limit) {
        this.limit = limit;
    }

    @Override
    public CompletionStage<byte[]> getBody() {
        return result;
    }

    @Override
    public void onSubscribe(Flow.Subscription newSubscription) {
        if (subscription != null) {
            newSubscription.cancel();
            return;
        }
        subscription = newSubscription;
        newSubscription.request(1);
    }

    @Override
    public void onNext(List<ByteBuffer> buffers) {
        if (result.isDone()) {
            return;
        }
        for (ByteBuffer buffer : buffers) {
            if (buffer.remaining() > limit - bytes.size()) {
                if (subscription != null) {
                    subscription.cancel();
                }
                result.completeExceptionally(new MdsFailure(MdsFailure.Reason.RESPONSE_SIZE));
                return;
            }
            byte[] chunk = new byte[buffer.remaining()];
            buffer.get(chunk);
            bytes.writeBytes(chunk);
        }
        if (subscription != null) {
            subscription.request(1);
        }
    }

    @Override
    public void onError(Throwable error) {
        result.completeExceptionally(MdsFailure.safe(error));
    }

    @Override
    public void onComplete() {
        result.complete(bytes.toByteArray());
    }
}
