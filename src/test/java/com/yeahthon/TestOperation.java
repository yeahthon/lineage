package com.yeahthon;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

// TODO 所有Operation的名称
// TODO 本地连接集群Flink操作表
public class TestOperation implements Serializable {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        FileSource<String> sqlSource = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("sql/hot.sql")
                )
                .build();

        env
                .fromSource(
                        sqlSource,
                        WatermarkStrategy.noWatermarks(),
                        "sql-text-source"
                )
                .map(new RichMapFunction<String, List<Operation>>() {
                    private transient Parser parser;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        StreamExecutionEnvironment localEnv = StreamExecutionEnvironment.getExecutionEnvironment(conf);
                        StreamTableEnvironmentImpl tableEnvImpl = (StreamTableEnvironmentImpl) StreamTableEnvironment.create(localEnv);

                        this.parser = tableEnvImpl.getParser();
                    }

                    @Override
                    public List<Operation> map(String sql) throws Exception {
                        return parser.parse(sql);
                    }
                })
                .flatMap(new FlatMapFunction<List<Operation>, String>() {
                    @Override
                    public void flatMap(List<Operation> operationList, Collector<String> out) throws Exception {
                        operationList.forEach(operation -> out.collect(operation.asSummaryString()));
                    }
                })
                .print();

        env.execute();
    }
}
