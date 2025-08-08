#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#
# Extracts Asciidoc code blocks, passing each one to a unix command provided by CHECK_CMD.
#
# If INC_LABELLED_CBS is comma separate listed of attribute names/values.  If INC_LABELLED_CBS is set to 'A=B', codeblocks
# with attribute A will only be extracted if the attribute value is B. Codeblocks without attribute A are extracted regardless.

BEGIN {CODEBLOCK = 0; SNIPPET = 0; EXIT_CODE = 0}
BEGINFILE {
    if (!CHECK_CMD) {
        print "CHECK_CMD variable is required"
        exit 1
    }
    if (!BLOCKTYPE) {
        print "BLOCKTYPE variable is required"
        exit 1
    }

    split(INC_LABELLED_CBS, includedAttrList, ",")
    for(idx in includedAttrList){
        split(includedAttrList[idx], pair, "=")
        includedAttrs[pair[1]] = pair[2]
    }

    if (ERRNO != "") {
        print "couldn't open " FILENAME " skipping"
        nextfile
    }
}
$0 ~ "^\\[source," BLOCKTYPE ",?.*\\]"                   {
     split(gensub(/^\[(.+)\]$/, "\\1", "g", $0), attrs, ",")
     blockIncluded = 1;
     for(key in attrs) {
          split(attrs[key], pair, "=")
          attrName = pair[1]
          attrValue = gensub(/^"(.+)"$/, "\\1", "g", pair[2])
          if ( attrName in includedAttrs && (includedAttrs[attrName] != attrValue)) {
              blockIncluded = 0;
              break
          }
     }

     if (blockIncluded) {
         SNIPPET = FNR;
     }
     next
} # code block follows between `----`
SNIPPET && !CODEBLOCK && /[-]{4}/                        { CODEBLOCK = 1; COUNTER = COUNTER++ ; next} # code block found
CODEBLOCK && /[-]{4}/                                    {
    print buf |& CHECK_CMD
    close(CHECK_CMD, "to")
    while ((CHECK_CMD |& getline outbuf) > 0) {
       if (outbuf) print outbuf
    }
    ext = close(CHECK_CMD, "from")
    if (ext != 0) {
        printf ( "Invalid %s snippet between lines %s and %s of %s \n", BLOCKTYPE, SNIPPET, FNR, FILENAME)
        EXIT_CODE = 1
    }
    CODEBLOCK = 0
    SNIPPET = 0
    buf = ""
} # code block terminated
CODEBLOCK                                                { buf = buf $0 ORS }
ENDFILE {
    #    print "validated " BLOCKTYPE " in "  FILENAME
}
END {exit EXIT_CODE}
