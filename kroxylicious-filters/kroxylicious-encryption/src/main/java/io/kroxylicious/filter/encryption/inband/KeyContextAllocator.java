/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import javax.security.auth.DestroyFailedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.filter.encryption.EncryptionException;

import edu.umd.cs.findbugs.annotations.Nullable;

import static java.util.stream.Collectors.toSet;

/**
 * The idea is the KeyContextAllocator becomes responsible allocating a DEK to
 * a client that wants to encrypt N records (creating a new DEK if required). The CompletionStage
 * for a DEK is allocated while there are encryptions and time remaining. A DEK is marked as closed
 * at allocation time if it's been exhausted or exceeds it's TTL. We use reference counting to attempt to
 * ensure that the underlying DEK is only destroyed once all clients are finished.
 * The allocator trusts that clients will use the DEK for the number of encryption operations requested
 * and expects that clients will cease using the KeyContext after they call `free()` on it. Therefore,
 * it should be safe to destroy a DEK once all clients have called free() and there are no references to the
 * DEK.
 * The allocator can destroy dead DEKs during allocation if there are keys with no references that
 * have expired or cannot satisfy the number of encryptions requested. The allocator can destroy dead
 * keys during a call to KeyContext#free() if the free results in the key having zero references.
 */
public class KeyContextAllocator {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyContextAllocator.class);
    private int dekId = 0;

    private record DekContext(AtomicInteger remainingEncryptions, long expiry, AtomicInteger references, CompletionStage<KeyContext> context,
                              AtomicBoolean closed) {
        // this is stateful to provide the atomic operation on remainingEncryptions
        public boolean maybeAllocate(int numEncryptions, long currentNanos) {
            if (closed.get()) {
                return false;
            }
            if (currentNanos >= expiry) {
                return false;
            }
            int beforeSubtraction = remainingEncryptions.getAndAdd(-numEncryptions);
            if (beforeSubtraction == numEncryptions) {
                // after subtraction there will be 0 available encryptions, so we can close it now
                closed.set(true);
            }
            boolean isAllocated = beforeSubtraction >= numEncryptions;
            if (isAllocated) {
                allocate();
            }
            return isAllocated;
        }

        public boolean isDead() {
            return closed.get() && references.get() <= 0;
        }

        public void destroy() {
            context.thenAccept(keyContext -> {
                try {
                    keyContext.destroy();
                }
                catch (DestroyFailedException e) {
                    // todo add back in the logic to reduce this expected error logging
                    LOGGER.warn("Failed to destroy a KeyContext. "
                            + "This event can happen because the JRE's SecretKeySpec class does not override destroy().", e);
                }
            });
        }

        public void allocate() {
            this.references.incrementAndGet();
        }

        public void deallocate() {
            this.references.decrementAndGet();
        }
    }

    private final Map<Integer, DekContext> idToDekContext = new HashMap<>();
    private final Function<Runnable, CompletionStage<KeyContext>> keyContextFunction;
    private final int maxEncryptionsPerDek;
    private final long dekTtlNanos;

    public KeyContextAllocator(Function<Runnable, CompletionStage<KeyContext>> keyContextFunction, int maxEncryptionsPerDek, long dekTtlNanos) {
        this.keyContextFunction = keyContextFunction;
        this.maxEncryptionsPerDek = maxEncryptionsPerDek;
        this.dekTtlNanos = dekTtlNanos;
    }

    // synchronized to get this working, could come up with something fancier. maybe it's not a big concern as on the hot path the work should mostly
    // be on the single netty eventloop for the channel.
    synchronized CompletionStage<KeyContext> allocate(int numEncryptions) {
        if (numEncryptions > maxEncryptionsPerDek) {
            throw new EncryptionException("cannot encrypt a batch larger than " + maxEncryptionsPerDek + " requested : " + numEncryptions);
        }
        if (numEncryptions < 0) {
            throw new EncryptionException("cannot encrypt a negative number of records");
        }
        DekContext allocated = maybeAllocateExistingDek(numEncryptions);
        if (allocated == null) {
            allocated = createAndAllocateNewDek(numEncryptions);
        }
        cleanupDeadDeks();
        return allocated.context;
    }

    private DekContext createAndAllocateNewDek(int numEncryptions) {
        int id = dekId++;
        CompletionStage<KeyContext> contextFuture = keyContextFunction.apply(() -> {
            this.free(id);
        });
        int remainingEncryptions = maxEncryptionsPerDek - numEncryptions;
        DekContext context = new DekContext(new AtomicInteger(remainingEncryptions), System.nanoTime() + dekTtlNanos, new AtomicInteger(0),
                contextFuture, new AtomicBoolean(remainingEncryptions == 0));
        context.allocate();
        idToDekContext.put(id, context);
        return context;
    }

    @Nullable
    private DekContext maybeAllocateExistingDek(int numEncryptions) {
        for (DekContext context : idToDekContext.values()) {
            if (context.maybeAllocate(numEncryptions, System.nanoTime())) {
                return context;
            }
            else {
                // we mark exhausted/timed-out contexts so that they can be cleaned later. this is because they may still be in use for encryption
                context.closed.set(true);
            }
        }
        return null;
    }

    private synchronized void cleanupDeadDeks() {
        Set<Map.Entry<Integer, DekContext>> dead = idToDekContext.entrySet().stream().filter(e -> e.getValue().isDead()).collect(toSet());
        for (Map.Entry<Integer, DekContext> contextEntry : dead) {
            contextEntry.getValue().destroy();
            idToDekContext.remove(contextEntry.getKey());
        }
    }

    private void free(int id) {
        DekContext dekContext = idToDekContext.get(id);
        dekContext.deallocate();
        if (dekContext.isDead()) {
            cleanupDeadDeks();
        }
    }

}
