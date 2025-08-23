package com.yeahthon;

import com.yeahthon.lineage.enums.SqlTypeEnum;
import com.yeahthon.lineage.operation.Operations;
import org.apache.calcite.schema.StreamableTable;
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
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;

import java.io.Serializable;
import java.util.*;

/**
 * Unit test for simple App.
 */
public class AppTest implements Serializable {
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
