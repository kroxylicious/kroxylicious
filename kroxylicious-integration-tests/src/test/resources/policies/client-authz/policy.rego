# OPA Policy for ClientAuthzIT integration tests
# This policy matches the ACL rules defined in ClientAuthzIT:
# - alice can do ALL operations on topicA and topicB
# - bob can CREATE topicA
# - alice can do ALL operations on group1, group2, and txnIdP
#
# Note: The policy uses the topics map for all resource types (topics, groups, transactional IDs)
# since the OPA authorizer passes resourceName as a string and the policy checks by name.

package kroxylicious.abac

default allow := false

# METADATA
# entrypoint: true

# Allow if user is owner of the resource and action is allowed for owner
allow if {
	user_is_owner
	action_allowed_for_owner
}

# Allow if user can access any resource (for CREATE operations like bob on topicA)
allow if {
	user_can_access_any_resource
	action_allowed_for_admin
}

# Check if user is owner of a resource (topic, group, or transactional ID)
user_is_owner if {
	some p in input.subject.principals
	p.type == "User"
	p.name == data.topics[input.resourceName].owner
}

# Check if user can access any resource (for CREATE operations)
user_can_access_any_resource if {
	some p in input.subject.principals
	p.type == "User"
	count([u |
		some u in data.anyResourceUsers
		u == p.name
	]) > 0
}

# Actions allowed for owners (all operations)
action_allowed_for_owner if input.action in {
	"create", "read", "write", "delete", "describe", 
	"describe_configs", "alter", "alter_configs"
}

# Actions allowed for admin/anyResourceUsers (CREATE and DELETE)
action_allowed_for_admin if input.action in {"create", "delete"}

