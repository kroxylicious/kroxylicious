# Protocol Logging Filter

A wire-level protocol trace for Kafka traffic passing through the proxy.
It logs every field of every Kafka request and response for the configured API keys, as version-aware JSON.
This is not a curated view of what a client is doing — it is a dump of what was on the wire.
Intended for debugging and demos.

## Enabling output

The filter is silent by default.
Two things must agree for output to appear:

1. The filter's own `logLevel` config (default `DEBUG`).
2. The logging backend must be enabled at that level for the logger `io.kroxylicious.filter.protocollogging`.

Setting one without the other produces no output at all.
With default settings, adding the filter to your proxy config and seeing nothing is expected — the logging backend must also be told to emit DEBUG for this logger.

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
      - name: io.kroxylicious.filter.protocollogging
        level: DEBUG           # change to WARN or OFF to silence
        additivity: false
        AppenderRef:
          - ref: STDOUT
```

`monitorInterval` is what makes this work without a restart.
Edit the file, change the level, and within `monitorInterval` seconds the proxy starts or stops emitting protocol traces.

The default `log4j2.yaml` shipped with `kroxylicious-app` does **not** include `monitorInterval`.
An operator must add it to enable live tuning.

Other SLF4J backends work too — the only requirement is a logger named `io.kroxylicious.filter.protocollogging` enabled at the configured level.
Log4j 2 is the example because it is what `kroxylicious-app` uses by default.

## Configuration

```yaml
filterDefinitions:
  - name: protocol-logger
    type: ProtocolLogging
    config:
      logLevel: DEBUG          # default; must match the backend level
      apiKeyNames:             # absent or empty = all API keys
        - METADATA
        - PRODUCE
        - FETCH
      maxBodyChars: 8192       # default; must be > 0
defaultFilters:
  - protocol-logger
```

| Field | Type | Default | Description |
|---|---|---|---|
| `logLevel` | SLF4J Level | `DEBUG` | The level at which the filter emits log messages. |
| `apiKeyNames` | List of strings | all | Kafka `ApiKeys` enum names to log. Absent or empty means all. |
| `maxBodyChars` | int | `8192` | Maximum characters in the JSON body before truncation. Must be greater than zero. |

When a body exceeds `maxBodyChars`, it is cut and a marker is appended:

```
<truncated: 45231 more chars>
```

The envelope line is never truncated.

## Security

The bodies of credential-bearing API keys are never logged.
The following API keys are hardcoded as excluded:

- `SASL_AUTHENTICATE`
- `CREATE_DELEGATION_TOKEN`
- `ALTER_USER_SCRAM_CREDENTIALS`
- `DESCRIBE_DELEGATION_TOKEN`

This is deliberately not configurable.
If you have a use case that requires logging these bodies, raise an issue.

For these API keys, the converter is never called — the body is structurally withheld, not redacted after the fact.
The envelope is still emitted so the handshake remains visible; only the body is replaced with a marker:

```
REQUEST  SASL_AUTHENTICATE v2  corr=2147483642  client=producer-1
<body withheld: credential-bearing API>
```

Record payloads (the content of Produce and Fetch messages) are not logged in readable form.
Kafka's generated JSON converters emit an empty byte array for records-typed fields regardless of content,
so the binary payload never appears in the output.

## Output format

Each log entry has two parts: a human-readable envelope line, then the JSON body.

**Request:**
```
REQUEST  METADATA v13  corr=1  client=producer-1
{
  "topics" : [ {
    "topicId" : "AAAAAAAAAAAAAAAAAAAAAA",
    "name" : "test-logging"
  } ],
  "allowAutoTopicCreation" : true,
  "includeTopicAuthorizedOperations" : false
}
```

**Response:**
```
RESPONSE METADATA v13  corr=1
{
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
```

Structured key-values are appended by the logging framework after the body.
These include `apiKey`, `apiVersion`, `clientCorrelationId`, `clientId`, `direction`, and `sessionId`.

## Pairing requests and responses

Use the `sessionId` structured key-value together with the correlation ID (`clientCorrelationId`).
Correlation IDs are only unique within a single connection, so `sessionId` is required to pair correctly across concurrent connections.

`sessionId` is deliberately not in the human-readable envelope — it is proxy-internal state, not part of the protocol trace.
It appears only in the structured key-values.

Latency can be derived from the log timestamps of matched request/response pairs.

## Caveats

- **Truncated bodies are not valid JSON.** If the body exceeds `maxBodyChars`, the output is cut at an arbitrary character position — braces and string literals may be left unbalanced — so it will not parse with `jq` or similar tools.
- **Idle consumers produce verbose output.** On an idle consumer the output is dominated by `HEARTBEAT` and `FETCH` responses. Use `apiKeyNames` to filter these out.
- **No cost for excluded API keys.** Messages for API keys not in the configured set are not decoded at all, so there is no performance cost for them.
