#parse("File Header.java")
#if (${PACKAGE_NAME} && ${PACKAGE_NAME} != "")package ${PACKAGE_NAME};#end

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ${NAME} {
  ${BODY}
}