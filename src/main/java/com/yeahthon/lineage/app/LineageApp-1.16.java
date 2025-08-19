//package com.yeahthon.lineage;
//
//import com.google.common.collect.ImmutableMap;
//import com.yeahthon.lineage.bean.FunctionResult;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.api.internal.TableEnvironmentImpl;
//import org.apache.flink.table.catalog.AbstractCatalog;
//import org.apache.flink.table.functions.AggregateFunction;
//import org.apache.flink.table.functions.ScalarFunction;
//import org.apache.flink.table.functions.TableAggregateFunction;
//import org.apache.flink.table.functions.TableFunction;
//import org.apache.flink.table.planner.delegation.ParserImpl;
//
//import java.util.Map;
//import java.util.Set;
//
//@Slf4j
//public class LineageApp {
//    private static final String SHOW_CREATE_TABLE_SQL = "show create table %s.`%s`.%s";
//    private static final String SHOW_CREATE_VIEW_SQL = "show create view %s.`%s`.%s";
//
//    private static final Map<String, String> FUNCTION_SUFFIX_MAP = ImmutableMap.of(
//            // org.apache.flink.table.functions.ScalarFunction
//            ScalarFunction.class.getName(), "udf",
//            TableFunction.class.getName(), "udtf",
//            AggregateFunction.class.getName(), "udaf",
//            TableAggregateFunction.class.getName(), "udtaf"
//    );
//
//    private final TableEnvironmentImpl tableEnv;
//
//    public LineageApp() {
//        Configuration configuration = new Configuration();
//        configuration.setBoolean("table.dynamic-table-options", true);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
//
//        EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                .inStreamingMode()
//                .build();
//
//        this.tableEnv = (TableEnvironmentImpl) StreamTableEnvironment.create(env, settings);
//    }
//
//    // 用来访问外部数据源
//    // TODO 可以一次为接口来接入HiveSQL
//    public void useCatalog(AbstractCatalog catalog) {
//        if (!tableEnv.getCatalog(catalog.getName()).isPresent()) {
//            tableEnv.registerCatalog(catalog.getName(), catalog);
//        }
//    }
//
//    public void execute(String singleSQL) {
//        log.info("Execute SQL:{}", singleSQL);
//
//        tableEnv.executeSql(singleSQL);
//    }
//
//    public Set<FunctionResult> analyzeFunction(String singleSQL) {
//        log.info("Analyze function in SQL:{}", singleSQL);
//
//        // 获取SQL解析器
//        ParserImpl parser = (ParserImpl) tableEnv.getParser();
//
//        // 解析SQL获取抽象语法树 Abstract syntax tree
//        parser.pa
//
//        // 校验语法合法性
//    }
//
//    public static void main(String[] args) {
//        System.out.println(ScalarFunction.class.getName());
//    }
//}
