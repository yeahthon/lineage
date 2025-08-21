package com.yeahthon.lineage.enums;

import java.util.regex.Pattern;

// 该枚举类有两个作用：
// 1、根据传入的SQL语句，判断该语句属于哪种操作类型，即type
// 2、判断传入的操作类型与传入的SQL语句，是否相匹配
public enum SqlTypeEnum {
    SELECT("SELECT", "^SELECT.*"),

    CREATE("CREATE", "^CREATE(?!\\s+TABLE.*AS SELECT).*$"),

    DROP("DROP", "^DROP.*"),

    ALTER("ALTER", "^ALTER.*"),

    INSERT("INSERT", "^INSERT.*"),

    DESC("DESC", "^DESC.*"),

    DESCRIBE("DESCRIBE", "^DESCRIBE.*"),

    EXPLAIN("EXPLAIN", "^EXPLAIN.*"),

    USE("USE", "^USE.*"),

    SHOW("SHOW", "^SHOW.*"),

    LOAD("LOAD", "^LOAD.*"),

    UNLOAD("UNLOAD", "^UNLOAD.*"),

    SET("SET", "^SET.*"),

    RESET("RESET", "^RESET.*"),

    EXECUTE("EXECUTE", "^EXECUTE.*"),

    ADD_JAR("ADD_JAR", "^ADD\\s+JAR\\s+\\S+"),
    ADD("ADD", "^ADD\\s+CUSTOMJAR\\s+\\S+"),
    ADD_FILE("ADD_FILE", "^ADD\\s+FILE\\s+\\S+"),

    PRINT("PRINT", "^PRINT.*"),

    CTAS("CTAS", "^CREATE\\s.*AS\\sSELECT.*$"),

    WITH("WITH", "^WITH.*"),

    UNKNOWN("UNKNOWN", "^UNKNOWN.*");

    private String type;
    private Pattern pattern;

    SqlTypeEnum(String type, String regex) {
        this.type = type;
        // 将给定的正则表达式字符串编译为一个Pattern对象
        // Pattern.CASE_INSENSITIVE:不区分大小写
        // Pattern.DOTALL:使.元字符匹配任何字符，包括行终止符（如换行符 \n），这样就可以匹配到多行的一条SQL语句
        this.pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public boolean match(String statement) {
        return pattern.matcher(statement).matches();
    }
}
