# PR #3738 Review Comments

---

## Top-level review comment

Good work — the rewrite addresses the architectural concerns from the first round and the
structure is much cleaner. A few more things worth sorting before this merges, mostly around
keeping the PCSM in its lane.

The PCSM's role is to be a session state machine: external events arrive via `on*()` methods,
it records what that means for the session, and tells the handlers what to do via `in*()`
callbacks. It shouldn't own lifecycle decisions — those should come from outside. The drain
code mostly respects this but has a few places where the PCSM reaches beyond its role.
Inline comments below cover the specifics.

One longer-term question — whether `DrainCoordinator` should be per-virtual-cluster rather
than a global registry — is blocked on a broader refactor and shouldn't hold this PR up.
Happy to raise an issue for it separately.

---

## Reply to existing thread — `ProxyChannelStateMachine.java` line 456
### (hrishabhg: "is there already not a channel descriptor?")

Uzziee, I think you and hrishabhg are talking past each other here. hrishabhg isn't
questioning whether to log channel addresses — he's asking whether there's already an
established identifier for this. There is: `sessionId`. It's already used in this class
at lines 383 and 784 for exactly this purpose, and the project logging policy says it
should always be present.

Replace the `frontendChannel`/`backendChannel` address keys with `sessionId` throughout
the drain logging:

```java
// Instead of:
.addKeyValue("frontendChannel", frontendChannelAddress())
.addKeyValue("backendChannel", backendChannelAddress())

// Use:
.addKeyValue("sessionId", kafkaSession.sessionId())
```

Once that's done, `frontendChannelAddress()` and `backendChannelAddress()` (lines 302–319)
can be deleted entirely.

Also on line 609: `log("...timeoutMs : {}", timeout.toMillis())` uses message interpolation,
which isn't the convention here. Use `addKeyValue("timeoutMs", timeout.toMillis())` instead.

---

## Reply to existing thread — `ProxyChannelStateMachine.java` line 486
### (hrishabhg: "we should not have this check if everything is forwarded to backend")

Agreed — if autoRead is disabled when draining starts, this path should be unreachable.
The defensive guard is papering over a concern that deserves a proper answer.

That said, before removing it, the counter increment directly above is the real problem
(see the thread on line 519).

---

## Reply to existing thread — `ProxyChannelStateMachine.java` line 519
### (hrishabhg: "clientMessageInFlight++ and then releasing the msg do not tie together")

hrishabhg is right, and this is worth spelling out because it's a correctness bug.

`clientMessageInFlight` is incremented before the state check, so it's incremented even
in the Draining branch where the message is immediately released. The counter is only
ever decremented in `messageFromServer` — when a response arrives back from the broker.
But since the message was dropped and never forwarded, no response will ever arrive.

The drain completion check is `if (clientMessageInFlight <= 0)`. With the counter one too
high, that condition can never trigger. The connection will hang in Draining indefinitely —
even after all genuinely in-flight requests have completed — and will only close when the
timeout force-kills it.

The defensive guard the comment describes as "this shouldn't happen" silently corrupts the
drain counter when it does.

(And yes, import `io.netty.util.ReferenceCountUtil` rather than using the fully-qualified name.)

---

## Reply to existing thread — `ProxyChannelStateMachine.java` line 587
### (hrishabhg nit: suggest `toDraining` name)

The name does need to change, but `toDraining` isn't quite right either. All `to*()` methods
in this class are private — they're internal transition steps. The public/package-private entry
points are all `on*()`: `onClientActive`, `onServerActive`, `onClientInactive` etc. The state
being entered here is `Draining`, so the right name is `onDraining`. That keeps the convention
consistent throughout.

---

## Reply to existing thread — `ProxyChannelStateMachine.java` line 595
### (hrishabhg: null check may cause PCSM to skip Closed transition)

hrishabhg's concern is valid — if `ch` is null the timeout is never scheduled and the
connection could hang in Draining indefinitely. This is addressed by the architectural
change suggested in the new inline comment on `startDraining` below: the timeout moves
to `DrainCoordinator` entirely, and the PCSM stops scheduling its own timers. The null
check goes away with it.

---

## New inline comment — `ProxyChannelState.Draining` + `startDraining`

Two related things here that are worth pulling together.

**The PCSM is making a lifecycle decision it shouldn't own.**

In `messageFromServer`, when the in-flight counter hits zero the PCSM calls `toClosed`
directly. That's the PCSM deciding "I'm done draining, close now." The PCSM should
evaluate the condition; something external should decide the policy.

**The timeout scheduling is also wrong.**

`startDraining` schedules its own force-close timer. There's no incoming event here —
the PCSM is proactively initiating a future action against itself. DrainCoordinator chose
the timeout value; it should own the timer.

**The fix: inject the completion action into the `Draining` state.**

```java
record Draining(Runnable onDrained) implements ProxyChannelState {}
```

`onDraining` accepts a `Runnable` and stores it in the state. When the counter hits zero,
the PCSM calls `draining.onDrained().run()` rather than `toClosed` directly. `DrainCoordinator`
injects what that means — completing the per-connection future, cancelling the timeout.

The PCSM also needs a package-private `onDrainTimeout()` for DrainCoordinator to call when
its timer fires:

```java
void onDrainTimeout() {
    toClosed(null, DisconnectCause.DRAIN_TIMEOUT);
}
```

Note: `DisconnectCause.DRAIN_TIMEOUT` is fine to keep in the PCSM — it's just a label that
travels through `toClosed` for metrics, exactly like `IDLE_TIMEOUT` does today. The PCSM
doesn't need to know anything about timers to record why it was told to close.

**The result:** `drainFuture` and `drainTimeoutFuture` fields are removed from the PCSM
entirely. `DrainCoordinator.drainCluster` becomes:

```java
for (ProxyChannelStateMachine pcsm : new ArrayList<>(connections)) {
    CompletableFuture<Void> closedFuture = new CompletableFuture<>();
    closedFutures.add(closedFuture);

    pcsm.executeOnEventLoop(() -> pcsm.onDraining(closedFuture::complete));

    someScheduler.schedule(
        () -> pcsm.executeOnEventLoop(() -> {
            pcsm.onDrainTimeout();
            closedFuture.complete(null);
        }),
        timeout.toMillis(), TimeUnit.MILLISECONDS);
}
```

Testing becomes much easier too — no futures, no channels, just pass a flag-setter as
`onDrained` and assert it was called.

---

## New inline comment — `KafkaProxy.shutdown()` — shutdown sequence is inverted

The proposal ([016](https://github.com/kroxylicious/design/blob/main/proposals/016-virtual-cluster-lifecycle.md), Graceful Shutdown, step 1) is explicit: "The Netty graceful shutdown is initiated, establishing the hard time limit for the entire shutdown sequence." Clusters then drain within that window.

The current implementation has it backwards. `drainAllClusters()` calls `.join()`, blocking completely before `shutdownGracefully()` is called. Netty never races with drain — it starts after drain finishes, providing no backstop against the drain itself. Worst-case shutdown is `drainTimeout` + Netty timeout, not just Netty timeout.

The fix is to start Netty shutdown first, then drain concurrently:

```java
if (proxyEventGroup != null) {
    // Start Netty shutdown — establishes the hard time limit (non-blocking)
    var closeFutures = proxyEventGroup.shutdownGracefully(quietPeriod, nettyTimeout, NANOSECONDS);

    // Drain concurrently within the Netty window.
    // If drain finishes first, Netty has nothing left to force-close.
    // If Netty timeout fires first, it force-closes remaining connections,
    // which triggers toClosed → pendingDrainCallback → closedFuture.complete(),
    // unblocking drainAllClusters naturally.
    drainAllClusters();

    closeFutures.forEach(Future::syncUninterruptibly);
}
if (managementEventGroup != null) {
    managementEventGroup.shutdownGracefully().forEach(Future::syncUninterruptibly);
}
```

This also implies the default `drainTimeout` needs to change. Once Netty's timer is the hard backstop, a per-cluster `drainTimeout` larger than Netty's shutdown timeout (default 15s) would never fire — Netty force-closes first. For the per-connection timer to be the graceful close path (recording `DRAIN_TIMEOUT`, closing cleanly) before Netty's hard kill, `drainTimeout` must be less than the Netty shutdown timeout. The current 30s default has the relationship backwards.

---

## New inline comment — `ProxyConfig` / `VirtualCluster` — drain timeout is at the wrong level

The proposal is explicit on placement and rationale ([016](https://github.com/kroxylicious/design/blob/main/proposals/016-virtual-cluster-lifecycle.md), Graceful Shutdown):

> The drain timeout is configurable per virtual cluster.
> ```yaml
> virtualClusters:
> - name: "my-cluster"
>   drainTimeout: 10s
> ```
> Different clusters may have different workload profiles, and per-cluster configuration provides the right foundation for reload, where only the affected cluster drains and a proxy-wide timeout would be unnecessarily coarse.

`drainTimeout` should move to `VirtualCluster`. `drainAllClusters()` already iterates clusters individually — it just needs to pass each cluster's own timeout to `drainCoordinator.drainCluster(name, cluster.drainTimeout())`.

With `drainTimeout` moved, `ProxyConfig` has no fields. `onVirtualClusterStopped` (also from the proposal) isn't implemented in this PR. The `proxy:` configuration block should not be introduced until there is something to put in it — the VC lifecycle PR deliberately left it out for the same reason. Remove `ProxyConfig`, `Configuration.proxy`, and the `proxy:` YAML key entirely from this PR. The serialization concern (round-tripped configs silently gaining a `proxy: { drainTimeout: 30s }` block) goes away with it.

---

## New inline comment — `DrainCoordinator.drainCluster` line 81
### (`pcsm.frontendChannel()`)

`DrainCoordinator` is calling `pcsm.frontendChannel()` purely to get a handle on the
event loop for dispatching. This reaches through the PCSM into its internal channel —
the PCSM should be telling the coordinator what to do, not handing out its internals.

Replace with a single method on PCSM:

```java
void executeOnEventLoop(Runnable task) {
    // delegates to frontendHandler's channel event loop internally
}
```

DrainCoordinator calls `pcsm.executeOnEventLoop(...)` throughout. `frontendChannel()`
can then be deleted.
