BEGIN {CODEBLOCK = 0; SNIPPET = 0; RED = "\\033[0;31m"; NO_COLOUR = "\\033[0m"; }
BEGINFILE {
    if (ERRNO != "") {
        print "couldn't open " FILENAME " skipping"
        nextfile
    }
}
/^\[source,yaml\]/                           { SNIPPET = 1; next} # code block follows between `----`
SNIPPET && !CODEBLOCK && /[-]{4}/            { CODEBLOCK = 1; COUNTER = COUNTER++ ; next} # code block found
CODEBLOCK && /[-]{4}/                        {
    CODEBLOCK = 0;
    SNIPPET = 0;
    yq_cmd = ("echo '" buf "' | yq 'true' > /dev/null")
    ext = system(yq_cmd);
    if (ext != 0) {
        print "Invalid YAML snippet at " FNR " of " FILENAME
    }
    SNIPPET = 0
    buf = ""
} # code block terminated
CODEBLOCK                                    { buf = buf $0 ORS }
ENDFILE {
#    print "validated YAML in "  FILENAME
}
