# OPA Authorizer

An authorization plugin for Kroxylicious that uses [Open Policy Agent (OPA)](https://www.openpolicyagent.org/) to evaluate authorization decisions using Rego policies.

## Why OPA?

While the ACL authorizer works well for simple authorization needs, OPA provides superior **expressiveness** for modeling authorization policies that align with how organizations actually think about access control.

### Expressiveness Over Enumeration

ACL authorizers require you to enumerate the *outcome* of a policy—every allowed operation must be explicitly listed. OPA's Rego language lets you express policies in terms that match natural language and organizational concepts.

### Resource Ownership Model

A common pattern in data streaming is the **"data as a product"** paradigm, where:
- Each topic has an **owner** (typically the team publishing business events)
- Topics may have **subscribers** (teams consuming the data)
- Administrative operations require privileged roles

With OPA, you can directly express this ownership model:

```rego
allow if {
    user_is_owner
    action_allowed_for_owner
}

user_is_owner if {
    data.topics[input.resourceName].owner == input.subject.principals[_].name
}
```

With ACLs, you'd need to pre-compute all possible owner relationships and enumerate them as individual ACL entries, making the policy harder to audit and maintain.

### Better Source of Truth

OPA policies are driven by JSON data files that serve as a rich source of truth:

```json
{
  "topics": {
    "invoices": {
      "owner": "finance-team",
      "subscribers": ["analytics-team", "audit-team"]
    }
  }
}
```

This format is more useful than ACL files because it:
- Clearly shows ownership relationships
- Can include metadata (email, slack handles, etc.)
- Separates policy logic from data
- Makes it easy to answer "who owns this topic?" vs. parsing ACLs

### Role-Based Abstractions

OPA policies can hide low-level Kafka operations behind meaningful roles. For example, a "subscriber" role automatically grants `DESCRIBE` and `READ` operations without requiring explicit enumeration of each operation in the data file.

## When to Use OPA vs ACL

Use **OPA** when:
- You need to model resource ownership or team-based access
- Your authorization logic benefits from expressing relationships rather than enumerating outcomes
- You want a rich data model that serves as both authorization source and organizational metadata
- You need role-based abstractions that hide low-level operations

Use **ACL** when:
- You have simple, static authorization requirements
- You prefer a straightforward, declarative format
- You don't need to model complex relationships

