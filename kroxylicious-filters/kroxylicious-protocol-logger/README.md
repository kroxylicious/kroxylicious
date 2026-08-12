# Protocol Logger Filter

A wire-level protocol trace for Kafka traffic passing through the proxy.
It logs every field of every Kafka request and response for the configured API keys, as version-aware JSON.
This is not a curated view of what a client is doing — it is a dump of what was on the wire.
Intended for debugging and demos.

## Enabling output

The filter is silent by default.
Two things must agree for output to appear:

1. The filter's own `logLevel` config (default `DEBUG`).
2. The logging backend must be enabled at that level for the logger used by the filter instance.

Setting one without the other produces no output at all.
With default settings, adding the filter to your proxy config and seeing nothing is expected — the logging backend must also be told to emit DEBUG for the filter's logger.

The simplest way to see output is to set the environment variable before starting the proxy:

```sh
KROXYLICIOUS_APP_LOG_LEVEL=DEBUG bin/kroxylicious-start.sh --config proxy-config.yaml
```

This enables DEBUG for all `io.kroxylicious` loggers.
For a more targeted approach, add a logger entry to `log4j2.yaml` (see below).

## Runtime log level tuning

The same filter config can be deployed to dev and prod.
Operators turn output on and off by tuning the logging backend — no proxy restart, no config change.

Add `monitorInterval` to the top of your `log4j2.yaml` so Log4j 2 re-reads the file periodically.
Then add a logger entry for the filter:

```yaml
Configuration:
  monitorInterval: 5          # re-read this file every 5 seconds
  # ... appenders unchanged ...
  Loggers:
    # ... other loggers ...
    Logger:
      - name: io.kroxylicious.filter.protocollogger.ProtocolLoggerFilter
        level: DEBUG           # change to WARN or OFF to silence
        additivity: false
        AppenderRef:
          - ref: STDOUT
```

`monitorInterval` is what makes this work without a restart.
Edit the file, change the level, and within `monitorInterval` seconds the proxy starts or stops emitting protocol traces.

The default `log4j2.yaml` shipped with `kroxylicious-app` does **not** include `monitorInterval`.
An operator must add it to enable live tuning.

When using a custom `loggerName` (see below), the logger entry must match that name instead.

Other SLF4J backends work too — the only requirement is a logger enabled at the configured level for the name used by the filter instance.
Log4j 2 is the example because it is what `kroxylicious-app` uses by default.

## Configuration

```yaml
filterDefinitions:
  - name: protocol-logger
    type: ProtocolLogger
    config:
      logLevel: DEBUG          # default; must match the backend level
      apiKeyNames:             # absent or empty = all API keys
        - METADATA
        - PRODUCE
        - FETCH
      loggerName: protocol.downstream  # default: the filter class name
defaultFilters:
  - protocol-logger
```

| Field | Type | Default | Description |
|---|---|---|---|
| `logLevel` | SLF4J Level | `DEBUG` | The level at which the filter emits log messages. |
| `apiKeyNames` | Set of strings | all | Kafka `ApiKeys` enum names to log. Absent or empty means all. Names are resolved case-insensitively and with non-alphanumeric characters stripped, so `FindCoordinator`, `find-coordinator` and `FIND_COORDINATOR` all work. |
| `loggerName` | String | filter class name | The SLF4J logger name used by this filter instance. |

## Two-instance example

An operator may place an instance at each end of the filter chain to see the effect of the filters between them.
Distinct logger names let each instance be enabled independently from the logging backend.

```yaml
filterDefinitions:
  - name: log-downstream
    type: ProtocolLogger
    config:
      loggerName: protocol.downstream
  - name: log-upstream
    type: ProtocolLogger
    config:
      loggerName: protocol.upstream
defaultFilters:
  - log-downstream
  - some-other-filter
  - log-upstream
```

## Security

The bodies of credential-bearing API keys are never logged.
The following API keys are hardcoded as excluded:

- `SASL_AUTHENTICATE`
- `CREATE_DELEGATION_TOKEN`
- `RENEW_DELEGATION_TOKEN`
- `EXPIRE_DELEGATION_TOKEN`
- `ALTER_USER_SCRAM_CREDENTIALS`
- `DESCRIBE_DELEGATION_TOKEN`

This is deliberately not configurable.
If you have a use case that requires logging these bodies, raise an issue.

For these API keys, the converter is never called — the body is structurally withheld, not redacted after the fact.
The header is still emitted so the handshake remains visible; only the payload is replaced with a null and the `payloadWithheld` field explains why:

```json
{
  "header" : {
    "type" : "REQUEST",
    "apiKey" : "SASL_AUTHENTICATE",
    "apiVersion" : 2,
    "correlationId" : 2147483642,
    "clientId" : "producer-1"
  },
  "payload" : null,
  "payloadWithheld" : "credential-bearing API"
}
```

Record payloads (the content of Produce and Fetch messages) are not logged in readable form.
Kafka's generated JSON converters emit an empty byte array for records-typed fields regardless of content,
so the binary payload never appears in the output.

## Output format

Each log entry is a single JSON object with a `header` and a `payload`.

**Request:**
```json
{
  "header" : {
    "type" : "REQUEST",
    "apiKey" : "METADATA",
    "apiVersion" : 13,
    "correlationId" : 1,
    "clientId" : "producer-1"
  },
  "payload" : {
    "topics" : [ {
      "topicId" : "AAAAAAAAAAAAAAAAAAAAAA",
      "name" : "test-logging"
    } ],
    "allowAutoTopicCreation" : true,
    "includeTopicAuthorizedOperations" : false
  }
}
```

**Response:**
```json
{
  "header" : {
    "type" : "RESPONSE",
    "apiKey" : "METADATA",
    "apiVersion" : 13,
    "correlationId" : 1
  },
  "payload" : {
    "throttleTimeMs" : 0,
    "brokers" : [ {
      "nodeId" : 1,
      "host" : "localhost",
      "port" : 9194,
      "rack" : null
    } ],
    "clusterId" : "MkU3OEVBNTcwNTJENDM2Qk",
    "controllerId" : 1,
    "topics" : [ {
      "errorCode" : 0,
      "name" : "test-logging",
      "topicId" : "n0bi9KA7TQSi6Bdf1Qb1Qg",
      "isInternal" : false,
      "partitions" : [ {
        "errorCode" : 0,
        "partitionIndex" : 0,
        "leaderId" : 1,
        "leaderEpoch" : 0,
        "replicaNodes" : [ 1 ],
        "isrNodes" : [ 1 ],
        "offlineReplicas" : [ ]
      } ],
      "topicAuthorizedOperations" : -2147483648
    } ],
    "errorCode" : 0
  }
}
```

Note that `clientId` appears only on requests — it does not exist on the wire for responses.
Responses omit the key entirely rather than emitting null.

Structured key-values are appended by the logging framework after the JSON body.
These include `apiKey`, `apiVersion`, `clientCorrelationId`, `clientId`, `direction`, and `sessionId`.

## Piping to jq

Each log entry is JSON, but a log **line** is not — the logging backend's timestamp, level, and logger name prefix surround it.
Piping raw log output to `jq` will fail unless you either strip the prefix or use a message-only appender.

To get machine-parseable output, add a message-only appender to your `log4j2.yaml`:

```yaml
Configuration:
  Appenders:
    Console:
      - name: PROTOCOL_TRACE
        PatternLayout:
          pattern: "%m%n"
  Loggers:
    Logger:
      - name: io.kroxylicious.filter.protocollogger.ProtocolLoggerFilter
        level: DEBUG
        additivity: false
        AppenderRef:
          - ref: PROTOCOL_TRACE
```

With that appender, each line is a complete JSON object and `jq` works directly:

```sh
# select METADATA entries (both request and response)
jq -c 'select(.header.apiKey == "METADATA")' < proxy-trace.log
```

## Pairing requests and responses

Use the `sessionId` structured key-value together with `.header.correlationId`.
Correlation IDs are only unique within a single connection, so `sessionId` is required to pair correctly across concurrent connections.

`sessionId` is deliberately not in the JSON entry — it is proxy-internal state, not part of the protocol trace.
It appears only in the structured key-values.

Correlation IDs in the output are not necessarily client correlation IDs.
Filters and routers can dispatch out-of-band requests upstream via `FilterContext#sendRequest` and `RouterContext#sendRequest`, which carry synthetic negative correlation IDs.
Depending on chain position this filter may observe them and cannot reliably tell them apart from client traffic, so pairing may produce entries with no counterpart.

Latency can be derived from the log timestamps of matched request/response pairs.

## Caveats

- **Idle consumers produce verbose output.** On an idle consumer the output is dominated by `HEARTBEAT` and `FETCH` responses. Use `apiKeyNames` to filter these out.
- **No cost for excluded API keys.** Messages for API keys not in the configured set are not decoded at all, so there is no performance cost for them.
