package com.yeahthon.lineage.constant;

import java.util.regex.Pattern;

public class LineageConstant {
    private static final String DELIMITER = ".";
    public static final Pattern GLOBAL_VARIABLE_PATTERN = Pattern.compile("\\$\\{(.+?)}");
    /** 分隔符 */
    public static final String SEPARATOR = ";\n";

    /** The define identifier of FlinkSQL Variable */
    public static final String VARIABLES = ":=";
    public static final String SEMICOLON = ";";
    public static final String SQL_SEPARATOR = ";\\s*(?:\\n|--.*)";
}
