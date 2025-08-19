package com.yeahthon.lineage.utils;

import org.apache.http.util.Asserts;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class AssertsUtil {
    private AssertsUtil() {
    }

    public static boolean isNotNull(Object object) {
        return object != null;
    }

    public static boolean isNull(Object object) {
        return object == null;
    }

    public static boolean isAllNotNull(Object... object) {
        return Arrays.stream(object).allMatch(AssertsUtil::isNotNull);
    }

    public static boolean isNullString(String str) {
        return isNull(str) || str.isEmpty();
    }

    public static boolean isAllNullString(String... str) {
        return Arrays.stream(str).allMatch(AssertsUtil::isNullString);
    }

    public static boolean isNotNullString(String str) {
        return !isNullString(str);
    }

    public static boolean isAllNotNullString(String... str) {
        return Arrays.stream(str).noneMatch(AssertsUtil::isNullString);
    }

    public static boolean isEquals(String str1, String str2) {
        return Objects.equals(str1, str2);
    }

    public static boolean isEqualsIgnoreCase(String str1, String str2) {
        return (str1 == null && str2 == null) || (str1 != null && str1.equalsIgnoreCase(str2));
    }

    public static boolean isNullCollection(Collection<?> collection) {
        return isNull(collection) || collection.isEmpty();
    }

    public static boolean isNotNullCollection(Collection<?> collection) {
        return !isNullCollection(collection);
    }

    public static boolean isNullMap(Map<?, ?> map) {
        return isNull(map) || map.isEmpty();
    }

    public static boolean isNotNullMap(Map<?, ?> map) {
        return !isNullMap(map);
    }

    public static boolean isContainsString(String str1, String str2) {
        return !isNullString(str1) && str1.contains(str2);
    }
}
