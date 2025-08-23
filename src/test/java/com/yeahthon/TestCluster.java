package com.yeahthon;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;

import java.util.Arrays;

public class TestCluster {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                "192.168.10.102",
                8081
        );
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        tableEnv.executeSql("create database test;");
        System.out.println(Arrays.toString(tableEnv.listDatabases()));

        env.execute();
    }
}
