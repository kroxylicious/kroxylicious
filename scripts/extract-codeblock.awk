#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#
BEGIN {CODEBLOCK = 0; SNIPPET = 0; RED = "\\033[0;31m"; NO_COLOUR = "\\033[0m"; }
BEGINFILE {
    if (ERRNO != "") {
        print "couldn't open " FILENAME " skipping"
        nextfile
    }
}
/^\[source,yaml\]/                           { SNIPPET = FNR; next} # code block follows between `----`
SNIPPET && !CODEBLOCK && /[-]{4}/            { CODEBLOCK = 1; COUNTER = COUNTER++ ; next} # code block found
CODEBLOCK && /[-]{4}/                        {
    yq_cmd = ("echo '" buf "' | yq 'true' > /dev/null")
    ext = system(yq_cmd);
    if (ext != 0) {
        printf "Invalid %s snippet between lines %s and %s of %s \n", BLOCKTYPE, SNIPPET, FNR, FILENAME
    }
    CODEBLOCK = 0;
    SNIPPET = 0;
    buf = ""
} # code block terminated
CODEBLOCK                                    { buf = buf $0 ORS }
ENDFILE {
#    print "validated YAML in "  FILENAME
}
