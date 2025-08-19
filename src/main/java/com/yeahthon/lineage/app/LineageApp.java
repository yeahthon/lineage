package com.yeahthon.lineage.app;

import com.yeahthon.lineage.bean.LineageColumnRel;
import com.yeahthon.lineage.bean.LineageResult;
import com.yeahthon.lineage.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;

import java.util.ArrayList;

public class LineageApp {
    private static StreamTableEnvironmentImpl tableEnv;

    public LineageResult handle(String statement) {
        System.out.println("Flink Version:1.17.0");

        ArrayList<LineageColumnRel> lineageRelList = new ArrayList<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        StreamTableEnvironmentImpl tableContext = (StreamTableEnvironmentImpl) tableEnv;

        String[] sqls = SQLUtil.getStatements(statement);
        for (String sql : sqls) {
            Operations
        }
    }
}
