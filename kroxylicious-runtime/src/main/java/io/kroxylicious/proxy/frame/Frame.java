/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A frame in the Kafka protocol, which may or may not be fully decoded.
 */
public interface Frame {

    /**
     * Number of bytes required for storing the frame length.
     */
    int FRAME_SIZE_LENGTH = Integer.BYTES;

    /** Sentinel indicating no specific target virtual node; the frame should be forwarded to the route's default node. */
    int NO_TARGET_VIRTUAL_NODE_ID = -1;

    /**
     * Estimate the expected encoded size in bytes of this {@code Frame}.<br>
     * In particular, written data by {@link #encode(ByteBufAccessor)} should be the same as reported by this method.
     * @return The estimated encoded size in bytes.
     */
    int estimateEncodedSize();

    /**
     * Write the frame, including the size prefix, to the given buffer
     * @param out The output buffer
     */
    void encode(ByteBufAccessor out);

    /**
     * The correlation id.
     * @return The correlation id.
     */
    int correlationId();

    /**
     * The api key id of this frame.
     * @return The api key id of this frame.
     */
    short apiKeyId();

    /**
     * The api version of this frame.
     * @return The api version of this frame.
     */
    short apiVersion();

    /**
     * Whether this frame has been decoded.
     * @return true if this frame is decoded, false otherwise.
     */
    boolean isDecoded();

    /**
     * This frame's current place in the routing/filter tree: either a {@link PathElement.RoutePosition}
     * (this frame's resolved address in the route tree) or a {@link PathElement.Originator} (this
     * frame is itself an in-flight, internally-issued request, anchored at a route position and
     * awaiting a promise). See {@link PathElement} for why those are different things.
     * <p>
     * This value is set wholesale at a handful of specific points in a frame's lifecycle, never
     * built up incrementally as the frame passes through each pipeline handler:
     * <ol>
     * <li><b>Unrouted:</b> {@code routing() == null} - e.g. a client request immediately after
     * decode, before its route has been resolved.</li>
     * <li><b>Client-forwarded request, routed:</b> once a route is resolved, {@code routing()} is
     * set once to that {@link PathElement.RoutePosition} and does not change again for the rest of
     * this frame's life.</li>
     * <li><b>Filter- or router-issued OOB request:</b> at issuance, {@code routing()} is set once
     * to a fresh {@link PathElement.Originator} anchored at the issuer's own route position. If
     * that request is then dispatched to a route it didn't already carry, the existing originator
     * is grafted onto the newly resolved route ({@link PathElement#graft}), preserving its
     * identity and promise - the one case where an in-flight frame's routing value is updated
     * rather than replaced outright.</li>
     * <li><b>Response arrival:</b> a response frame's {@code routing()} is always set to the exact
     * same value its request carried - copied verbatim for a locally synthesised OOB response, or
     * restored verbatim (via a separate, wire-level correlation id lookup - the real broker cannot
     * carry proxy metadata) for a response that made a round trip to a broker. It is never
     * reconstructed by walking {@link PathElement.RoutePosition#parent()} backwards.</li>
     * <li><b>Nested router boundary:</b> as an ordinary (non-OOB) response crosses back out of a
     * nested router's scope, {@code routing()} is reset to the outer route position that activated
     * that router - the one place a response's routing value is deliberately changed, rather than
     * copied or restored.</li>
     * </ol>
     *
     * @return this frame's routing value, or {@code null} if not yet routed.
     */
    default @Nullable PathElement routing() {
        return null;
    }

    /**
     * Sets this frame's current place in the routing/filter tree. See {@link #routing()} for when
     * and how this is set over a frame's lifecycle.
     * @param routing the routing value, or {@code null} if not routed.
     */
    default void setRouting(@Nullable PathElement routing) {
    }
}
