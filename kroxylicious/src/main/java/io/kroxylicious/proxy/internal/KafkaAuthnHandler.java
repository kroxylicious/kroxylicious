/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AlterClientQuotasRequestData;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.ControlledShutdownRequestData;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeDelegationTokenRequestData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EnvelopeRequestData;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.message.StopReplicaRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.UnregisterBrokerRequestData;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AllocateProducerIdsRequest;
import org.apache.kafka.common.requests.AlterClientQuotasRequest;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.AlterPartitionReassignmentsRequest;
import org.apache.kafka.common.requests.AlterPartitionRequest;
import org.apache.kafka.common.requests.AlterReplicaLogDirsRequest;
import org.apache.kafka.common.requests.AlterUserScramCredentialsRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.BrokerHeartbeatRequest;
import org.apache.kafka.common.requests.BrokerRegistrationRequest;
import org.apache.kafka.common.requests.ControlledShutdownRequest;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreateDelegationTokenRequest;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeClientQuotasRequest;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeDelegationTokenRequest;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeLogDirsRequest;
import org.apache.kafka.common.requests.DescribeProducersRequest;
import org.apache.kafka.common.requests.DescribeTransactionsRequest;
import org.apache.kafka.common.requests.DescribeUserScramCredentialsRequest;
import org.apache.kafka.common.requests.ElectLeadersRequest;
import org.apache.kafka.common.requests.EndQuorumEpochRequest;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EnvelopeRequest;
import org.apache.kafka.common.requests.ExpireDelegationTokenRequest;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchSnapshotRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.LeaderAndIsrRequest;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListPartitionReassignmentsRequest;
import org.apache.kafka.common.requests.ListTransactionsRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetDeleteRequest;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RenewDelegationTokenRequest;
import org.apache.kafka.common.requests.StopReplicaRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.UnregisterBrokerRequest;
import org.apache.kafka.common.requests.UpdateFeaturesRequest;
import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.authenticator.SaslInternalConfigs;
import org.apache.kafka.common.security.plain.internals.PlainSaslServerProvider;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.scram.internals.ScramSaslServerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import io.kroxylicious.proxy.frame.BareSaslRequest;
import io.kroxylicious.proxy.frame.BareSaslResponse;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.tag.VisibleForTesting;

/**
 * <p>A Netty handler that allows the proxy
 * to perform the Kafka SASL authentication exchanges needed by
 * a client connection, specifically {@code SaslHandshake},
 * and {@code SaslAuthenticate}.</p>
 *
 * <p>See the doc for {@link State} for a detailed state machine.</p>
 *
 * <p>Client software and authorization information thus obtained is propagated via
 * an {@link AuthenticationEvent} to upstream handlers, specifically {@link KafkaProxyFrontendHandler}, to use in
 * deciding how the connection to an upstream connection should be made.</p>
 *
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=51809888">KIP-12: Kafka Sasl/Kerberos and SSL implementation</a>
 * added support for Kerberos authentication"
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-43%3A+Kafka+SASL+enhancements">KIP-43: Kafka SASL enhancements</a>
 * added the SaslHandshake RPC in Kafka 0.10.0.0"
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-84%3A+Support+SASL+SCRAM+mechanisms">KIP-84: Support SASL SCRAM mechanisms</a>
 * added support for the SCRAM-SHA-256 and SCRAM-SHA-512 mechanisms"
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-152+-+Improve+diagnostics+for+SASL+authentication+failures">KIP-152: Improve diagnostics for SASL authentication failures</a>
 * added support for the SaslAuthenticate RPC (previously the auth bytes were not encapsulated in a Kafka frame"
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=75968876">KIP-255: OAuth Authentication via SASL/OAUTHBEARER</a>
 * added support for OAUTH authentication"
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-368%3A+Allow+SASL+Connections+to+Periodically+Re-Authenticate">KIP-365: Allow SASL Connections to Periodically Re-Authenticate</a>
 * added time-based reauthentication requirements for clients"
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-684+-+Support+mutual+TLS+authentication+on+SASL_SSL+listeners">KIP-684: Support mTLS authentication on SASL_SSL listeners</a>
 * added support for mutual TLS authentication even on SASL_SSL listeners (which was previously ignored)"
 */
public class KafkaAuthnHandler extends ChannelInboundHandlerAdapter {

    static {
        PlainSaslServerProvider.initialize();
        ScramSaslServerProvider.initialize();
    }

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAuthnHandler.class);

    /**
     * Represents a state in the {@link KafkaAuthnHandler} state machine.
     * <pre><code>
     *                            START
     *                              │
     *       ╭────────────────┬─────┴───────╮───────────────╮
     *       │                │             │               │
     *       ↓                ↓             │               ↓
     * API_VERSIONS ──→ SASL_HANDSHAKE_v0 ══╪══⇒ unframed_SASL_AUTHENTICATE
     *   │      │                           │                     │
     *   │      │             ╭─────────────╯                     │
     *   │      │             ↓                                   │
     *   │      ╰───→ SASL_HANDSHAKE_v1+ ──→ SASL_AUTHENTICATE    │
     *   │                                         │              ↓
     *   ╰─────────────────────────────────────────╰─────→ UPSTREAM_CONNECT
     * </code></pre>
     * <ul>
     * <li>API_VERSIONS if optional within the Kafka protocol</li>
     * <li>SASL authentication may or may not be in use</li>
     * </ul>
     */
    enum State {
        START,
        API_VERSIONS,
        SASL_HANDSHAKE_v0,
        SASL_HANDSHAKE_v1_PLUS,
        UNFRAMED_SASL_AUTHENTICATE,
        FRAMED_SASL_AUTHENTICATE,
        FAILED,
        AUTHN_SUCCESS
    }

    public enum SaslMechanism {
        PLAIN("PLAIN", null) {
            @Override
            public Map<String, Object> negotiatedProperties(SaslServer saslServer) {
                return Map.of();
            }
        },
        SCRAM_SHA_256("SCRAM-SHA-256",
                ScramMechanism.SCRAM_SHA_256) {
            @Override
            public Map<String, Object> negotiatedProperties(SaslServer saslServer) {
                Object lifetime = saslServer.getNegotiatedProperty(SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY);
                return lifetime == null
                        ? Map.of()
                        : Map.of(SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY, lifetime);
            }
        },
        SCRAM_SHA_512("SCRAM-SHA-512", ScramMechanism.SCRAM_SHA_512) {
            @Override
            public Map<String, Object> negotiatedProperties(SaslServer saslServer) {
                Object lifetime = saslServer.getNegotiatedProperty(SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY);
                return lifetime == null
                        ? Map.of()
                        : Map.of(SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY, lifetime);
            }
        };

        // TODO support OAUTHBEARER, GSSAPI
        private final String name;
        private final ScramMechanism scramMechanism;

        SaslMechanism(String saslName, ScramMechanism scramMechanism) {
            this.name = saslName;
            this.scramMechanism = scramMechanism;
        }

        public String mechanismName() {
            return name;
        }

        static SaslMechanism fromMechanismName(String mechanismName) {
            switch (mechanismName) {
                case "PLAIN":
                    return PLAIN;
                case "SCRAM-SHA-256":
                    return SCRAM_SHA_256;
                case "SCRAM-SHA-512":
                    return SCRAM_SHA_512;
            }
            throw new UnsupportedSaslMechanismException(mechanismName);
        }

        public ScramMechanism scramMechanism() {
            return scramMechanism;
        }

        public abstract Map<String, Object> negotiatedProperties(SaslServer saslServer);
    }

    private final List<String> enabledMechanisms;

    @VisibleForTesting
    SaslServer saslServer;

    private final Map<String, AuthenticateCallbackHandler> mechanismHandlers;

    @VisibleForTesting
    State lastSeen;

    public KafkaAuthnHandler(Channel ch,
                             Map<KafkaAuthnHandler.SaslMechanism, AuthenticateCallbackHandler> mechanismHandlers) {
        this(ch, State.START, mechanismHandlers);
    }

    @VisibleForTesting
    KafkaAuthnHandler(Channel ch,
                      State init,
                      Map<KafkaAuthnHandler.SaslMechanism, AuthenticateCallbackHandler> mechanismHandlers) {
        this.lastSeen = init;
        LOG.debug("{}: Initial state {}", ch, lastSeen);
        this.mechanismHandlers = mechanismHandlers.entrySet().stream().collect(Collectors.toMap(
                e -> e.getKey().mechanismName(), Map.Entry::getValue));
        this.enabledMechanisms = List.copyOf(this.mechanismHandlers.keySet());
    }

    private InvalidRequestException illegalTransition(State next) {
        InvalidRequestException e = new InvalidRequestException("Illegal state transition from " + lastSeen + " to " + next);
        lastSeen = State.FAILED;
        return e;
    }

    private void doTransition(Channel channel, State next) {
        State previous = lastSeen;
        switch (next) {
            case API_VERSIONS:
                if (previous != State.START) {
                    throw illegalTransition(next);
                }
                break;
            case SASL_HANDSHAKE_v0:
            case SASL_HANDSHAKE_v1_PLUS:
                if (previous != State.START
                        && previous != State.API_VERSIONS) {
                    throw illegalTransition(next);
                }
                break;
            case UNFRAMED_SASL_AUTHENTICATE:
                if (previous != State.START
                        && previous != State.SASL_HANDSHAKE_v0
                        && previous != State.UNFRAMED_SASL_AUTHENTICATE) {
                    throw illegalTransition(next);
                }
                break;
            case FRAMED_SASL_AUTHENTICATE:
                if (previous != State.SASL_HANDSHAKE_v1_PLUS
                        && previous != State.FRAMED_SASL_AUTHENTICATE) {
                    throw illegalTransition(next);
                }
                break;
            case AUTHN_SUCCESS:
                if (previous != State.FRAMED_SASL_AUTHENTICATE
                        && previous != State.UNFRAMED_SASL_AUTHENTICATE) {
                    throw illegalTransition(next);
                }
                break;
            case FAILED:
                break;
            default:
                throw illegalTransition(next);
        }
        LOG.debug("{}: Transition from {} to {}", channel, lastSeen, next);
        lastSeen = next;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof BareSaslRequest) {
            handleBareRequest(ctx, (BareSaslRequest) msg);
        }
        else if (msg instanceof DecodedRequestFrame) {
            handleFramedRequest(ctx, (DecodedRequestFrame<?>) msg);
        }
        else {
            throw new IllegalStateException("Unexpected message " + msg.getClass());
        }
    }

    private void handleFramedRequest(ChannelHandlerContext ctx, DecodedRequestFrame<?> frame) throws SaslException {
        switch (frame.apiKey()) {
            case API_VERSIONS:
                doTransition(ctx.channel(), State.API_VERSIONS);
                ctx.fireChannelRead(frame);
                return;
            case SASL_HANDSHAKE:
                doTransition(ctx.channel(), frame.apiVersion() == 0 ? State.SASL_HANDSHAKE_v0 : State.SASL_HANDSHAKE_v1_PLUS);
                onSaslHandshakeRequest(ctx, (DecodedRequestFrame<SaslHandshakeRequestData>) frame);
                return;
            case SASL_AUTHENTICATE:
                doTransition(ctx.channel(), State.FRAMED_SASL_AUTHENTICATE);
                onSaslAuthenticateRequest(ctx, (DecodedRequestFrame<SaslAuthenticateRequestData>) frame);
                return;
            default:
                if (lastSeen == State.AUTHN_SUCCESS) {
                    ctx.fireChannelRead(frame);
                }
                else {
                    writeFramedResponse(ctx, frame, errorResponse(frame, new IllegalSaslStateException("Not authenticated")));
                }
        }
    }

    private void handleBareRequest(ChannelHandlerContext ctx, BareSaslRequest msg) throws SaslException {
        // TODO support SASL GSS API
        if (lastSeen == State.SASL_HANDSHAKE_v0
                || lastSeen == State.UNFRAMED_SASL_AUTHENTICATE) {
            doTransition(ctx.channel(), State.UNFRAMED_SASL_AUTHENTICATE);
            // delegate to the SASL code to read the bytes directly
            writeBareResponse(ctx, doEvaluateResponse(ctx, msg.bytes()));
        }
        else {
            lastSeen = State.FAILED;
            throw new InvalidRequestException("Bare SASL bytes without GSSAPI support or prior SaslHandshake");
        }
    }

    private void writeBareResponse(ChannelHandlerContext ctx, byte[] bytes) throws SaslException {
        ctx.writeAndFlush(new BareSaslResponse(bytes));
    }

    private ApiMessage errorResponse(DecodedRequestFrame<?> frame, Throwable error) {
        /*
         * This monstrosity is needed because there isn't any _nicely_ abstracted code we can borrow from Kafka
         * which creates and response with error codes set appropriately.
         */
        final AbstractRequest req;
        ApiMessage reqBody = frame.body();
        short apiVersion = frame.apiVersion();
        switch (frame.apiKey()) {
            case SASL_HANDSHAKE:
            case SASL_AUTHENTICATE:
                // These should have been handled by our caller
                throw new IllegalStateException();
            case PRODUCE:
                req = new ProduceRequest((ProduceRequestData) reqBody, apiVersion);
                break;
            case FETCH:
                req = new FetchRequest((FetchRequestData) reqBody, apiVersion);
                break;
            case LIST_OFFSETS:
                ListOffsetsRequestData listOffsetsRequestData = (ListOffsetsRequestData) reqBody;
                if (listOffsetsRequestData.replicaId() == ListOffsetsRequest.CONSUMER_REPLICA_ID) {
                    req = ListOffsetsRequest.Builder.forConsumer(true, IsolationLevel.forId(listOffsetsRequestData.isolationLevel()), true)
                            .build(apiVersion);
                }
                else {
                    req = ListOffsetsRequest.Builder.forReplica(apiVersion, listOffsetsRequestData.replicaId())
                            .build(apiVersion);
                }
                break;
            case METADATA:
                req = new MetadataRequest((MetadataRequestData) reqBody, apiVersion);
                break;
            case OFFSET_COMMIT:
                req = new OffsetCommitRequest((OffsetCommitRequestData) reqBody, apiVersion);
                break;
            case OFFSET_FETCH:
                OffsetFetchRequestData offsetFetchRequestData = (OffsetFetchRequestData) reqBody;
                if (offsetFetchRequestData.groups() != null) {
                    req = new OffsetFetchRequest.Builder(
                            offsetFetchRequestData.groups().stream().collect(Collectors.toMap(
                                    OffsetFetchRequestData.OffsetFetchRequestGroup::groupId,
                                    x -> x.topics().stream().flatMap(
                                            t -> t.partitionIndexes().stream().map(
                                                    p -> new TopicPartition(t.name(), p)))
                                            .collect(Collectors.toList()))),
                            true, false)
                            .build(apiVersion);
                }
                else if (offsetFetchRequestData.topics() != null) {
                    req = new OffsetFetchRequest.Builder(
                            offsetFetchRequestData.groupId(),
                            offsetFetchRequestData.requireStable(),
                            offsetFetchRequestData.topics().stream().flatMap(
                                    x -> x.partitionIndexes().stream().map(
                                            p -> new TopicPartition(x.name(), p)))
                                    .collect(Collectors.toList()),
                            false)
                            .build(apiVersion);
                }
                else {
                    throw new IllegalStateException();
                }
                break;
            case FIND_COORDINATOR:
                req = new FindCoordinatorRequest.Builder((FindCoordinatorRequestData) reqBody)
                        .build(apiVersion);
                break;
            case JOIN_GROUP:
                req = new JoinGroupRequest((JoinGroupRequestData) reqBody, apiVersion);
                break;
            case HEARTBEAT:
                req = new HeartbeatRequest.Builder((HeartbeatRequestData) reqBody)
                        .build(apiVersion);
                break;
            case LEAVE_GROUP:
                LeaveGroupRequestData data = (LeaveGroupRequestData) reqBody;
                req = new LeaveGroupRequest.Builder(data.groupId(), data.members())
                        .build(apiVersion);
                break;
            case SYNC_GROUP:
                req = new SyncGroupRequest((SyncGroupRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_GROUPS:
                req = new DescribeGroupsRequest.Builder((DescribeGroupsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case LIST_GROUPS:
                req = new ListGroupsRequest((ListGroupsRequestData) reqBody, apiVersion);
                break;
            case API_VERSIONS:
                req = new ApiVersionsRequest((ApiVersionsRequestData) reqBody, apiVersion);
                break;
            case CREATE_TOPICS:
                req = new CreateTopicsRequest((CreateTopicsRequestData) reqBody, apiVersion);
                break;
            case DELETE_TOPICS:
                req = new DeleteTopicsRequest.Builder((DeleteTopicsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case DELETE_RECORDS:
                req = new DeleteRecordsRequest.Builder((DeleteRecordsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case INIT_PRODUCER_ID:
                req = new InitProducerIdRequest.Builder((InitProducerIdRequestData) reqBody)
                        .build(apiVersion);
                break;
            case OFFSET_FOR_LEADER_EPOCH:
                req = new OffsetsForLeaderEpochRequest((OffsetForLeaderEpochRequestData) reqBody, apiVersion);
                break;
            case ADD_PARTITIONS_TO_TXN:
                req = new AddPartitionsToTxnRequest((AddPartitionsToTxnRequestData) reqBody, apiVersion);
                break;
            case ADD_OFFSETS_TO_TXN:
                req = new AddOffsetsToTxnRequest((AddOffsetsToTxnRequestData) reqBody, apiVersion);
                break;
            case END_TXN:
                req = new EndTxnRequest.Builder((EndTxnRequestData) reqBody)
                        .build(apiVersion);
                break;
            case WRITE_TXN_MARKERS:
                req = new WriteTxnMarkersRequest.Builder((WriteTxnMarkersRequestData) reqBody)
                        .build(apiVersion);
                break;
            case TXN_OFFSET_COMMIT:
                req = new TxnOffsetCommitRequest((TxnOffsetCommitRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_ACLS:
                DescribeAclsRequestData d = (DescribeAclsRequestData) reqBody;
                req = new DescribeAclsRequest.Builder(new AclBindingFilter(
                        new ResourcePatternFilter(
                                ResourceType.fromCode(d.resourceTypeFilter()),
                                d.resourceNameFilter(),
                                PatternType.fromCode(d.patternTypeFilter())),
                        new AccessControlEntryFilter(
                                d.principalFilter(),
                                d.hostFilter(),
                                AclOperation.fromCode(d.operation()),
                                AclPermissionType.fromCode(d.permissionType()))))
                        .build(apiVersion);
                break;
            case CREATE_ACLS:
                req = new CreateAclsRequest.Builder((CreateAclsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case DELETE_ACLS:
                req = new DeleteAclsRequest.Builder((DeleteAclsRequestData) reqBody).build(apiVersion);
                break;
            case DESCRIBE_CONFIGS:
                req = new DescribeConfigsRequest((DescribeConfigsRequestData) reqBody, apiVersion);
                break;
            case ALTER_CONFIGS:
                req = new AlterConfigsRequest((AlterConfigsRequestData) reqBody, apiVersion);
                break;
            case ALTER_REPLICA_LOG_DIRS:
                req = new AlterReplicaLogDirsRequest((AlterReplicaLogDirsRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_LOG_DIRS:
                req = new DescribeLogDirsRequest((DescribeLogDirsRequestData) reqBody, apiVersion);
                break;
            case CREATE_PARTITIONS:
                req = new CreatePartitionsRequest.Builder((CreatePartitionsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case CREATE_DELEGATION_TOKEN:
                req = new CreateDelegationTokenRequest.Builder((CreateDelegationTokenRequestData) reqBody)
                        .build(apiVersion);
                break;
            case RENEW_DELEGATION_TOKEN:
                req = new RenewDelegationTokenRequest.Builder((RenewDelegationTokenRequestData) reqBody)
                        .build(apiVersion);
                break;
            case EXPIRE_DELEGATION_TOKEN:
                req = new ExpireDelegationTokenRequest.Builder((ExpireDelegationTokenRequestData) reqBody)
                        .build(apiVersion);
                break;
            case DESCRIBE_DELEGATION_TOKEN:
                DescribeDelegationTokenRequestData tokenRequestData = (DescribeDelegationTokenRequestData) reqBody;
                req = new DescribeDelegationTokenRequest.Builder(
                        tokenRequestData.owners().stream().map(o -> new KafkaPrincipal(o.principalType(), o.principalName())).collect(Collectors.toList()))
                        .build(apiVersion);
                break;
            case DELETE_GROUPS:
                req = new DeleteGroupsRequest((DeleteGroupsRequestData) reqBody, apiVersion);
                break;
            case ELECT_LEADERS:
                ElectLeadersRequestData electLeaders = (ElectLeadersRequestData) reqBody;
                req = new ElectLeadersRequest.Builder(
                        ElectionType.valueOf(electLeaders.electionType()),
                        electLeaders.topicPartitions().stream().flatMap(
                                t -> t.partitions().stream().map(
                                        p -> new TopicPartition(t.topic(), p)))
                                .collect(Collectors.toList()),
                        electLeaders.timeoutMs())
                        .build(apiVersion);
                break;
            case INCREMENTAL_ALTER_CONFIGS:
                req = new IncrementalAlterConfigsRequest((IncrementalAlterConfigsRequestData) reqBody, apiVersion);
                break;
            case ALTER_PARTITION_REASSIGNMENTS:
                req = new AlterPartitionReassignmentsRequest.Builder((AlterPartitionReassignmentsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case LIST_PARTITION_REASSIGNMENTS:
                req = new ListPartitionReassignmentsRequest.Builder((ListPartitionReassignmentsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case OFFSET_DELETE:
                req = new OffsetDeleteRequest((OffsetDeleteRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_CLIENT_QUOTAS:
                req = new DescribeClientQuotasRequest((DescribeClientQuotasRequestData) reqBody, apiVersion);
                break;
            case ALTER_CLIENT_QUOTAS:
                req = new AlterClientQuotasRequest((AlterClientQuotasRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_USER_SCRAM_CREDENTIALS:
                req = new DescribeUserScramCredentialsRequest.Builder((DescribeUserScramCredentialsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case ALTER_USER_SCRAM_CREDENTIALS:
                req = new AlterUserScramCredentialsRequest.Builder((AlterUserScramCredentialsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case DESCRIBE_QUORUM:
                req = new DescribeClientQuotasRequest((DescribeClientQuotasRequestData) reqBody, apiVersion);
                break;
            case ALTER_PARTITION:
                req = new AlterPartitionRequest((AlterPartitionRequestData) reqBody, apiVersion);
                break;
            case UPDATE_FEATURES:
                req = new UpdateFeaturesRequest((UpdateFeaturesRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_CLUSTER:
                req = new DescribeClusterRequest((DescribeClusterRequestData) reqBody, apiVersion);
                break;
            case DESCRIBE_PRODUCERS:
                req = new DescribeProducersRequest.Builder((DescribeProducersRequestData) reqBody)
                        .build(apiVersion);
                break;
            case DESCRIBE_TRANSACTIONS:
                req = new DescribeTransactionsRequest.Builder((DescribeTransactionsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case LIST_TRANSACTIONS:
                req = new ListTransactionsRequest.Builder((ListTransactionsRequestData) reqBody)
                        .build(apiVersion);
                break;
            case ALLOCATE_PRODUCER_IDS:
                req = new AllocateProducerIdsRequest((AllocateProducerIdsRequestData) reqBody, apiVersion);
                break;
            case VOTE:
                req = new VoteRequest.Builder((VoteRequestData) reqBody)
                        .build(apiVersion);
                break;
            case BEGIN_QUORUM_EPOCH:
                req = new BeginQuorumEpochRequest.Builder((BeginQuorumEpochRequestData) reqBody)
                        .build(apiVersion);
                break;
            case END_QUORUM_EPOCH:
                req = new EndQuorumEpochRequest.Builder((EndQuorumEpochRequestData) reqBody)
                        .build(apiVersion);
                break;
            case ENVELOPE:
                req = new EnvelopeRequest((EnvelopeRequestData) reqBody, apiVersion);
                break;
            case FETCH_SNAPSHOT:
                req = new FetchSnapshotRequest((FetchSnapshotRequestData) reqBody, apiVersion);
                break;
            case LEADER_AND_ISR:
                LeaderAndIsrRequestData lisr = (LeaderAndIsrRequestData) reqBody;
                req = new LeaderAndIsrRequest.Builder(apiVersion, lisr.controllerId(),
                        lisr.controllerEpoch(), lisr.brokerEpoch(),
                        lisr.ungroupedPartitionStates(),
                        lisr.topicStates().stream().collect(Collectors.toMap(
                                LeaderAndIsrRequestData.LeaderAndIsrTopicState::topicName,
                                LeaderAndIsrRequestData.LeaderAndIsrTopicState::topicId)),
                        lisr.liveLeaders().stream().map(
                                x -> new Node(
                                        x.brokerId(),
                                        x.hostName(),
                                        x.port()))
                                .collect(Collectors.toList()))
                        .build(apiVersion);
                break;
            case STOP_REPLICA:
                StopReplicaRequestData stopReplica = (StopReplicaRequestData) reqBody;
                req = new StopReplicaRequest.Builder(apiVersion,
                        stopReplica.controllerId(),
                        stopReplica.controllerEpoch(),
                        stopReplica.brokerEpoch(),
                        stopReplica.deletePartitions(),
                        stopReplica.topicStates())
                        .build(apiVersion);
                break;
            case UPDATE_METADATA:
                req = new UpdateFeaturesRequest((UpdateFeaturesRequestData) reqBody, apiVersion);
                break;
            case CONTROLLED_SHUTDOWN:
                req = new ControlledShutdownRequest.Builder((ControlledShutdownRequestData) reqBody, apiVersion)
                        .build(apiVersion);
                break;
            case BROKER_REGISTRATION:
                req = new BrokerRegistrationRequest((BrokerRegistrationRequestData) reqBody, apiVersion);
                break;
            case BROKER_HEARTBEAT:
                req = new BrokerHeartbeatRequest((BrokerHeartbeatRequestData) reqBody, apiVersion);
                break;
            case UNREGISTER_BROKER:
                req = new UnregisterBrokerRequest((UnregisterBrokerRequestData) reqBody, apiVersion);
                break;
            default:
                throw new IllegalStateException();
        }
        return req.getErrorResponse(error).data();
    }

    private void onSaslHandshakeRequest(ChannelHandlerContext ctx,
                                        DecodedRequestFrame<SaslHandshakeRequestData> data)
            throws SaslException {
        String mechanism = data.body().mechanism();
        Errors error;
        if (lastSeen == State.AUTHN_SUCCESS) {
            error = Errors.ILLEGAL_SASL_STATE;
        }
        else if (enabledMechanisms.contains(mechanism)) {
            var cbh = mechanismHandlers.get(mechanism);
            // TODO use SNI to supply the correct hostname
            saslServer = Sasl.createSaslServer(mechanism, "kafka", null, null, cbh);
            if (saslServer == null) {
                throw new IllegalStateException("SASL mechanism had no providers: " + mechanism);
            }
            else {
                error = Errors.NONE;
            }
        }
        else {
            error = Errors.UNSUPPORTED_SASL_MECHANISM;
        }

        SaslHandshakeResponseData body = new SaslHandshakeResponseData()
                .setMechanisms(enabledMechanisms)
                .setErrorCode(error.code());
        writeFramedResponse(ctx, data, body);

    }

    private void onSaslAuthenticateRequest(ChannelHandlerContext ctx,
                                           DecodedRequestFrame<SaslAuthenticateRequestData> data) {
        byte[] bytes = new byte[0];
        Errors error;
        String errorMessage;

        try {
            bytes = doEvaluateResponse(ctx, data.body().authBytes());
            error = Errors.NONE;
            errorMessage = null;
        }
        catch (SaslAuthenticationException e) {
            error = Errors.SASL_AUTHENTICATION_FAILED;
            errorMessage = e.getMessage();
        }
        catch (SaslException e) {
            error = Errors.SASL_AUTHENTICATION_FAILED;
            errorMessage = "An error occurred";
        }

        SaslAuthenticateResponseData body = new SaslAuthenticateResponseData()
                .setErrorCode(error.code())
                .setErrorMessage(errorMessage)
                .setAuthBytes(bytes);
        // TODO add support for session lifetime
        writeFramedResponse(ctx, data, body);
    }

    private static void writeFramedResponse(ChannelHandlerContext ctx,
                                            DecodedRequestFrame<?> data, ApiMessage body) {
        ctx.writeAndFlush(
                new DecodedResponseFrame<>(
                        data.apiVersion(),
                        data.correlationId(),
                        new ResponseHeaderData().setCorrelationId(data.correlationId()),
                        body));
    }

    private byte[] doEvaluateResponse(ChannelHandlerContext ctx,
                                      byte[] authBytes)
            throws SaslException {
        final byte[] bytes;
        try {
            bytes = saslServer.evaluateResponse(authBytes);
        }
        catch (SaslAuthenticationException e) {
            LOG.debug("{}: Authentication failed", ctx.channel());
            doTransition(ctx.channel(), State.FAILED);
            saslServer.dispose();
            throw e;
        }
        catch (Exception e) {
            LOG.debug("{}: Authentication failed", ctx.channel());
            doTransition(ctx.channel(), State.FAILED);
            saslServer.dispose();
            throw new SaslAuthenticationException(e.getMessage());
        }

        if (saslServer.isComplete()) {
            try {
                String authorizationId = saslServer.getAuthorizationID();
                var properties = SaslMechanism.fromMechanismName(saslServer.getMechanismName()).negotiatedProperties(saslServer);
                doTransition(ctx.channel(), State.AUTHN_SUCCESS);
                LOG.debug("{}: Authentication successful, authorizationId={}, negotiatedProperties={}", ctx.channel(), authorizationId, properties);
                ctx.fireUserEventTriggered(new AuthenticationEvent(authorizationId, properties));
            }
            finally {
                saslServer.dispose();
            }
        }
        return bytes;
    }
}
