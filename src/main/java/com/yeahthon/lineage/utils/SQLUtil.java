package com.yeahthon.lineage.utils;


import com.yeahthon.lineage.constant.LineageConstant;

// SQL工具类，将一个包含多个SQL语句的脚本按照自定义(默认为;)切分为数组
public class SQLUtil {
    private SQLUtil() {
    }

    public static String[] getStatements(String sql) {
        return getStatements(sql, LineageConstant.SQL_SEPARATOR);
    }

    public static String[] getStatements(String multiSql, String sqlSeparator) {
        // SQL is null
        if (AssertsUtil.isNullString(multiSql)) {
            return new String[0];
        }

        // 匹配以;结尾的SQL语句，或者以注释结尾的SQL语句
        final String localSQLSeparator = ";\\s*(?:\\n|--.*)";
        String[] splits = multiSql.replace("\r\n", "\n").split(localSQLSeparator);
        String lastStatement = splits[splits.length - 1].trim();
        if (lastStatement.endsWith(LineageConstant.SEMICOLON)) {
            splits[splits.length - 1] = lastStatement.substring(0, lastStatement.length() - 1);
        }

        return splits;
    }
}
