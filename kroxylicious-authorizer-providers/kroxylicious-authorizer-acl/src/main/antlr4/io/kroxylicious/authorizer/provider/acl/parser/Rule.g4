grammar Rule;

rule: versionStmt
    importStmt*
    //denyRule*
    allowRule*
    endRule
    <EOF>
    ;

versionStmt: 'version' '1' SEMI
    ;

importStmt: IMPORT name=IDENT (AS local=IDENT)? FROM packageName SEMI
    ;

packageName: qualIdent
    ;
qualIdent: IDENT (DOT IDENT)*
    ;

denyRule: DENY allowOrDenyRule SEMI
    ;
allowRule: ALLOW allowOrDenyRule SEMI
    ;

allowOrDenyRule: userPattern TO operationPattern
    ;

userPattern: principalType WITH NAME namePred
    ;
principalType: IDENT
    ;
namePred: STAR # wildcardOp
    | nameEq # eqOp
    | nameIn # inOp
    | nameMatch # matchOp
    | nameLike # likeOp
    ;
nameEq: EQ STRING
    ;
nameIn: IN LBRA STRING (COMMA STRING)* RBRA
    ;
nameMatch: MATCHING REGEX
    ;
nameLike: LIKE STRING
    ;

operationPattern: operations resource WITH NAME namePred
    ;
operation: IDENT
    ;
resource: IDENT
    //| STAR
    ;
operations: STAR
    | operation
    | operationSet
    ;
operationSet: LBRA operation (COMMA operation)* RBRA
    ;

endRule: OTHERWISE DENY SEMI
    ;

// lexer rules
SEMI: ';';
DOT: '.';
COMMA: ',';
STAR: '*';
EQ: '=';
LBRA: '{';
RBRA: '}';
DENY: 'deny';
ALLOW: 'allow';
OTHERWISE: 'otherwise';
IN: 'in';
TO: 'to';
AS: 'as';
LIKE: 'like';
MATCHING: 'matching';
FROM: 'from';
IMPORT: 'import';
NAME: 'name';
WITH: 'with';
STRING: '"' (ESC | .)*? '"';
fragment ESC: '\\"' | '\\\\';
REGEX: '/' (ESC | .)*? '/';
LINE_COMMENT: '//' .*? '\r'? '\n' -> skip;
COMMENT: '/*' .*? '*/' -> skip;
WS: [ \t\r\n]+ -> skip;
IDENT: [A-Za-z][A-Za-z0-9_]*;
