/**
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
grammar PrometheusMetric;

// https://github.com/prometheus/docs/blob/main/docs/instrumenting/exposition_formats.md

file
    : line* EOF
    ;

line
    : metric CR?
    | CR
    ;

metric
    : metricName labelSet? value timestamp?
    ;

metricName
    : IDENTIFIER
    ;

labelSet
    : '{' labelList? '}'
    ;

labelList
    : label (',' label)*
    ;

label
    : IDENTIFIER '=' QUOTED_STRING
    ;

value
    : NUMBER
    | INF
    | NEGATIVE_INF
    | NAN
    ;

timestamp
    : NUMBER
    ;

QUOTED_STRING : '"' STRING_CHAR* '"' ;

NUMBER : MINUS? DIGITS ('.' DIGITS)? ([Ee] MINUS? DIGITS)? ;

MINUS        : '-'  ;
INF          : '+Inf';
NEGATIVE_INF : '-Inf';
NAN          : 'NaN';

fragment DIGITS : [0-9]+ ;
fragment STRING_CHAR // any char except ",\ and \n unless escaped
        : ~["\\\n]
        | '\\"'
        | '\\\\'
        | '\\n'
        ;

IDENTIFIER : [a-zA-Z_:][a-zA-Z0-9_:]*;
CR         : '\n';
WHITESPACE : [\r\t ]+  -> skip;
COMMENT    : '#' ~[\n]* '\n' -> skip;