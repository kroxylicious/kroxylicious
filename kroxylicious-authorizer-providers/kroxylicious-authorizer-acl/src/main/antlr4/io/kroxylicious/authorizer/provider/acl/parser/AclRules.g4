/**
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
grammar AclRules;

rule: versionStmt
    importStmt*
    denyRule*
    allowRule*
    endRule
    <EOF>
    ;

versionStmt: VERSION INT SEMI
    ;

// We include all the lexer keywords, in addition to the lexer IDENT token, into this grammar rule
// so that those keywords can be used in identifiers in the grammar
ident : VERSION
    | FROM | IMPORT | AS
    | ALLOW | DENY  | TO
    | WITH | NAME | IN | LIKE | MATCHING
    | OTHERWISE
    | IDENT;

importStmt: FROM packageName IMPORT importList SEMI
    ;
packageName: qualIdent
    ;
qualIdent: ident (DOT ident)*
    ;
importList: importElement (COMMA importElement)*
    ;
importElement: name=ident (AS local=ident)?
    ;

denyRule: DENY allowOrDenyRule SEMI
    ;
allowRule: ALLOW allowOrDenyRule SEMI
    ;

allowOrDenyRule: userPattern TO operationPattern
    ;

userPattern: principalType WITH NAME userNamePred
    ;
principalType: ident
    ;
userNamePred: nameAny
    | nameEq
    | nameIn
    | nameLike
    ;

namePred: nameAny
    | nameEq
    | nameIn
    | nameMatch
    | nameLike
    ;
nameAny: STAR
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

operations: STAR
    | operation
    | operationSet
    ;
operation: ident
    ;
operationSet: LBRA operation (COMMA operation)* RBRA
    ;

resource: ident
    //| STAR
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
VERSION: 'version';
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
STRING: '"' (STRING_ESC | .)*? '"';
fragment STRING_ESC: '\\"' | '\\\\';
REGEX: '/' (REGEX_ESC | .)*? '/';
fragment REGEX_ESC: '\\/' | '\\\\';
LINE_COMMENT: '//' .*? '\r'? '\n' -> skip;
COMMENT: '/*' .*? '*/' -> skip;
WS: [ \t\r\n]+ -> skip;
IDENT: [A-Za-z][A-Za-z0-9_]*;
INT: [1-9][0-9]*;