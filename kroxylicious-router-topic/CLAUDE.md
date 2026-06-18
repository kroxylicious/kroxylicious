# kroxylicious-router-topic - Claude Context

## Primary Documentation

See [README.md](README.md) for the router's design, routing model, version capping rationale, and limitations.

## For Claude

### Kafka message collection pitfall

`CreatableTopicResult`, `CreatePartitionsTopicResult`, and similar response types extend `ImplicitLinkedHashCollection.Element`. Each element has `prev`/`next` pointers and can only belong to one collection at a time. Moving an element from one collection to another corrupts the source's iterator.

When merging error responses into a final response, always iterate with `List.copyOf(collection)` and add with `element.duplicate()`:

```java
for (var tr : List.copyOf(errorResponse.topics())) {
    merged.topics().add(tr.duplicate());
}
```

### CreatePartitions null-vs-empty assignments

`CreatePartitionsTopic`'s constructor initialises `assignments` to `new ArrayList<>(0)` (not null). On the wire, an empty list serialises differently from null -- the broker interprets an empty list as "explicit empty assignment" and returns `INVALID_REPLICA_ASSIGNMENT`. When constructing test fixtures, always call `.setAssignments(null)` for automatic assignment.

### MetadataDecomposer has a non-standard signature

`MetadataDecomposer` does not implement `RequestDecomposer<Req, Resp>`. Its `decompose()` and `recompose()` methods take additional parameters (`defaultRoute`, `table`) because METADATA has three request variants (all-topics, broker-info-only, specific topics) that require different routing logic.

### Test execution

Unit tests: `mvn test -pl kroxylicious-router-topic`

Integration tests live in `kroxylicious-integration-tests` and extend `TopicPartitionRoutingBaseIT`. They require failsafe:

```sh
mvn verify -pl kroxylicious-integration-tests \
    -Dit.test="CreateTopicsRoutingIT,DeleteTopicsRoutingIT" \
    -Dfailsafe.failIfNoSpecifiedTests=false
```

If the router module has been modified, rebuild it first:

```sh
mvn install -pl kroxylicious-router-topic -am -DskipTests -q
```
