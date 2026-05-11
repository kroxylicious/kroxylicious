# PCSM Logging Compliance Handover

`ProxyChannelStateMachine` predates the project logging policy and is not fully compliant.
This file tracks the changes needed. The drain PR (#3738) adds further violations on top.

## Logging policy summary (relevant rules)

- Always include `sessionId` if known — it is the primary session correlator
- Always include `virtualCluster` when the event is cluster-specific
- Use `addKeyValue()` for all structured context — never `addArgument()` or `{}` interpolation
- Use `address` rather than separate `host` + `port` keys

## Pre-existing violations

### `illegalState` (line 431)
Missing `sessionId` and `virtualCluster`. This is the log operators reach for when something
goes wrong — it needs both.

```java
// Current
LOGGER.atError()
        .addKeyValue("state", state)
        .addKeyValue("message", msg)
        .log("Unexpected event, closing channels with no client response");

// Required
LOGGER.atError()
        .addKeyValue("sessionId", kafkaSession.sessionId())
        .addKeyValue("virtualCluster", clusterName())
        .addKeyValue("state", state)
        .addKeyValue("message", msg)
        .log("Unexpected event, closing channels with no client response");
```

### `onServerException` (line 647)
Missing `sessionId` and `virtualCluster`.

```java
// Add to existing log:
.addKeyValue("sessionId", kafkaSession.sessionId())
.addKeyValue("virtualCluster", clusterName())
```

### `onClientException` — both branches (lines 673, 681)
Missing `sessionId` and `virtualCluster` in both the oversized-frame branch and the general branch.

```java
// Add to both existing logs:
.addKeyValue("sessionId", kafkaSession.sessionId())
.addKeyValue("virtualCluster", clusterName())
```

### `onClientActive` (line 382)
Missing `virtualCluster`. Also uses separate `remoteHost`/`remotePort` keys — policy says prefer `address`.

```java
// Current
.addKeyValue("remoteHost", ...)
.addKeyValue("remotePort", ...)

// Required
.addKeyValue("virtualCluster", clusterName())
.addKeyValue("address", this.frontendHandler.remoteHost() + ":" + this.frontendHandler.remotePort())
```

### `toConnecting` (line 783)
Missing `virtualCluster`. Uses separate `clientHost`/`clientPort` keys — same issue as above.

```java
// Current
.addKeyValue("clientHost", ...)
.addKeyValue("clientPort", ...)

// Required
.addKeyValue("virtualCluster", clusterName())
.addKeyValue("clientAddress", this.frontendHandler.remoteHost() + ":" + this.frontendHandler.remotePort())
```

### `setState` TRACE (line 924)
Missing `sessionId`.

```java
// Add:
.addKeyValue("sessionId", kafkaSession.sessionId())
```

## Suggested approach

1. Raise a separate issue: "PCSM logging does not comply with project logging policy"
2. Fix pre-existing violations in one PR against main (small, no behaviour change)
