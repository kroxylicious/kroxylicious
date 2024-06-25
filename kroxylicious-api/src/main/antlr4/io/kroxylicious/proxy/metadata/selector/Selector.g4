grammar Selector;

selector
  : requirement (COMMA requirement)* EOF
  ;

requirement
   : equalityRequirement
   | existsRequirement
   | inRequirement
   ;

equalityRequirement
   : label (EQ | EQEQ | NOTEQ) value
   ;

existsRequirement
   : NOT ? label
   ;
inRequirement
   : label ( 'in' | 'notin' ) set
   ;
set
   : OPEN_PAR value (COMMA value)* CLOSE_PAR
   ;
label
   : ( dnsSubdomain SLASH )? labelName
   ;
justLabel
   : label EOF
   ;

dnsSubdomain
   : dnsLabel ( DOT dnsLabel )*
   ;

labelName
   : letDig ( ( letDig | DASH | UNDERSCORE | DOT )* letDig )?
   | keywordKludge
   ;

value
   : // can be empty!
   | letDig ( ( letDig | DASH | UNDERSCORE | DOT )* letDig )?
   | keywordKludge
   ;

justValue
   : value EOF
   ;

dnsLabel   // rfc1035
    : LETTER ( ( letDigHyp )* letDig )?
    | keywordKludge
    ;

keywordKludge
   // workaround for copying with in and notin being keywords, but also an 'identifiers'
   : 'in'
   | 'notin'
   ;

letDigHyp // rfc1035
    : letDig
    | DASH
    ;
letDig // rfc1035
    : LETTER
    | DIGIT
    ;


DASH : '-' ;
UNDERSCORE : '_' ;
DOT : '.' ;
COMMA : ',' ;
OPEN_PAR : '(' ;
CLOSE_PAR : ')' ;
NOTEQ : '!=' ;
EQEQ : '==' ;
EQ : '=' ;
NOT : '!' ;
SLASH : '/' ;
LETTER: 'a'..'z' | 'A'..'Z' ;
DIGIT : '0'..'9' ;

WS : [ \t\r\n]+ -> skip ;
