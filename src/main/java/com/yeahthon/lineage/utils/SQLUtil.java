package com.yeahthon.lineage.utils;


import com.yeahthon.lineage.constant.LineageConstant;

public class SQLUtil {
    private SQLUtil() {
    }

    public static String[] getStatements(String sql) {
        return getStatements(sql, LineageConstant.SQL_SEPARATOR);
    }

    public static String[] getStatements(String sql, String sqlSeparator) {
        // SQL is null
        if (AssertsUtil.isNullString(sql)) {
            return new String[0];
        }

        // 匹配以；结尾的SQL语句，或者以注释结尾的SQL语句
        final String localSQLSeparator = ";\\s*(?:\\n|--.*)";
        String[] splits = sql.replace("\r\n", "\n").split(localSQLSeparator);
        String lastStatement = splits[splits.length - 1].trim();
        if (lastStatement.endsWith(LineageConstant.SEMICOLON)) {
            splits[splits.length - 1] = lastStatement.substring(0, lastStatement.length() - 1);
        }

        return splits;
    }
}
