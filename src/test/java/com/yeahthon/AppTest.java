package com.yeahthon;

import com.yeahthon.lineage.enums.SqlTypeEnum;

import java.util.Arrays;

/**
 * Unit test for simple App.
 */
public class AppTest {
    public static void main(String[] args) {
        SqlTypeEnum[] values = SqlTypeEnum.values();
        SqlTypeEnum value = values[0];
        System.out.println(value);
    }
}
