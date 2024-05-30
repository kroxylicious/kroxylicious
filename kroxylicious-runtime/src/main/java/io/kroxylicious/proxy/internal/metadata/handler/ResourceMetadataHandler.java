/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.metadata.handler;

import java.util.concurrent.CompletionStage;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.config.ResourceMetadata;
import io.kroxylicious.proxy.internal.metadata.ResourceMetadataFrame;
import io.kroxylicious.proxy.metadata.DescribeTopicLabelsRequest;
import io.kroxylicious.proxy.metadata.ListTopicsRequest;
import io.kroxylicious.proxy.metadata.ResourceMetadataRequest;
import io.kroxylicious.proxy.metadata.ResourceMetadataResponse;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A channel handler to implement the contract of {@link io.kroxylicious.proxy.filter.FilterContext#sendMetadataRequest(ResourceMetadataRequest)},
 * which can involve delegate resource metadata requests to external systems, including but not limited to the Kafka cluster.
 */
public class ResourceMetadataHandler extends ChannelOutboundHandlerAdapter {

    @NonNull
    private static TopicMetadataSource fromConfig(@NonNull ResourceMetadata metadataSource) {
        TopicMetadataSource topicMetadataSource = TopicMetadataSource.EMPTY;
        if (metadataSource.configSource() != null) {
            if (metadataSource.configSource().topicLabellings() != null) {
                topicMetadataSource = new StaticTopicMetadataSource(metadataSource.configSource().topicLabellings());
            }
        }
        return topicMetadataSource;
    }

    private final TopicMetadataSource topicMetadataSource;

    public ResourceMetadataHandler(@NonNull ResourceMetadata metadataSource) {
        this(fromConfig(metadataSource));
    }

    public ResourceMetadataHandler(@NonNull TopicMetadataSource topicMetadataSource) {
        this.topicMetadataSource = topicMetadataSource;
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof ResourceMetadataFrame<?, ?> frame) {
            CompletionStage cs;
            if (frame.request() instanceof DescribeTopicLabelsRequest req) {
                cs = topicMetadataSource.topicLabels(req.topicNames());
            }
            else if (frame.request() instanceof ListTopicsRequest req) {
                cs = topicMetadataSource.topicsMatching(req.topicNames(), req.selectors());
            }
            else {
                throw new IllegalStateException();
            }
            promise.setSuccess();
            delegateCompletion(cs, frame);
        }
        else {
            ctx.writeAndFlush(msg, promise);
        }

    }

    private static <Q extends ResourceMetadataRequest<R>, R extends ResourceMetadataResponse<Q>> void delegateCompletion(CompletionStage<R> mapCompletionStage,
                                                                                                                         ResourceMetadataFrame<Q, R> frame) {
        mapCompletionStage.whenComplete((result, error) -> {
            if (error != null) {
                frame.promise().completeExceptionally(error);
            }
            else {
                frame.promise().complete(result);
            }
        });
    }
}
