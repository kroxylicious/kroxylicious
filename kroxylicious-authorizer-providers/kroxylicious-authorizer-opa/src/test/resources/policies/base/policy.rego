# For example the policy might be:
# 1. every topic has a single "owner" subject,
# 2. the owner is allowed to read, write, alter and alter the configs of the topic.
# 3. topics may also have a set of subjects who are subscribers
# 4. Only a privileged admin can delete the topic

package kroxylicious.abac

default allow := false

# METADATA
# entrypoint: true
allow if {
	user_is_admin
	action_allowed_for_admin
}

allow if {
	user_is_owner
	action_allowed_for_owner
}

allow if {
	user_is_subscriber
	action_allowed_for_subscriber
}

user_is_admin if {
	count([p |
		some p in input.subject.principals
		p.type == "User"
		p.name == "admin"
	]) > 0
}

user_is_owner if {
	count([p |
		some p in input.subject.principals
		p.type == "User"
		p.name == data.topics[input.resourceName].owner
	]) > 0
}

user_is_subscriber if {
	count([p |
		some p in input.subject.principals
		p.type == "User"
		count([s |
			some s in data.topics[input.resourceName].subscribers
			s == p.name
		]) > 0
	]) > 0
}

action_allowed_for_admin if input.action in {"create", "delete"}
action_allowed_for_owner if input.action in {"read", "write", "describe", "describe_configs", "alter", "alter_configs"}
action_allowed_for_subscriber if input.action in {"read", "describe"}
