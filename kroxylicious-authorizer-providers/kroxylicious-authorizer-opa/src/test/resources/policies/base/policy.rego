# For example the policy might be:
# 1. every topic has a single "owner" subject,
# 2. the owner is allowed to read, write, alter and alter the configs of the topic.
# 3. topics may also have a set of subjects who are subscribers
# 4. Only a privileged admin can delete the topic

package kroxylicious.abac

# METADATA
# entrypoint: true
result := {
	"allowed": [action |
		some action in input.actions
		action_allowed(action)
	],
	"denied": [action |
		some action in input.actions
		not action_allowed(action)
	]
}

action_allowed(action) if {
	user_is_admin
	action.action in {"create", "delete"}
}

action_allowed(action) if {
	user_is_owner(action)
	action.action in {"read", "write", "describe", "describe_configs", "alter", "alter_configs"}
}

action_allowed(action) if {
	user_is_subscriber(action)
	action.action in {"read", "describe"}
}

user_is_admin if {
	count([p |
		some p in input.subject.principals
		p.type == "User"
		p.name == "admin"
	]) > 0
}

user_is_owner(action) if {
	count([p |
		some p in input.subject.principals
		p.type == "User"
		p.name == data.topics[action.resourceName].owner
	]) > 0
}

user_is_subscriber(action) if {
	count([p |
		some p in input.subject.principals
		p.type == "User"
		count([s |
			some s in data.topics[action.resourceName].subscribers
			s == p.name
		]) > 0
	]) > 0
}
