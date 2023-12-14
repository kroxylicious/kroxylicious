#!/usr/local/bin/expect
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#


set send_slow {1 .1}
log_file out
set prompt ".*(%|#|\\$) $"          ;# default prompt
catch {set prompt $env(EXPECT_PROMPT)}

spawn ssh -tt oslo.local


set send_human {.1 .3 1 .05 2}
send -s "echo hello"
send -s " got prompt\n"

send -s "echo hello got another prompt\n"

send -h "echo hello got another prompt\n"

send -h "echo hello got another prompt\n"


send -h "echo hello got another prompt\n"

send -- "exit\n"
expect eof



