package com.yeahthon;

import com.yeahthon.lineage.base.LineageConvert;
import com.yeahthon.lineage.bean.LineageColumnRel;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class TestHive {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "yeahthon");
        System.setProperty("hadoop.home.dir", "H:\\code\\lineage\\hadoop_home");
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        StreamTableEnvironmentImpl tableEnvImpl = (StreamTableEnvironmentImpl) tableEnv;
        env.setParallelism(1);

        HiveCatalog hiveCatalog = new HiveCatalog(
                "hive-catalog",
                "default",
                "H:\\code\\lineage\\conf",
                "3.1.3"
        );

        tableEnv.registerCatalog("hive-catalog", hiveCatalog);
        tableEnvImpl.useCatalog("hive-catalog");
        List<Operation> operationList = tableEnvImpl.getParser().parse("insert into test.student select s_id || s_name as s_id, s_name, s_birth, s_sex from test.student_back");
        Operation operation = operationList.get(0);
        SinkModifyOperation sinkOperation = (SinkModifyOperation) operation;
        // 获取查询操作
        PlannerQueryOperation queryOperation = (PlannerQueryOperation) sinkOperation.getChild();
        // 获取Calcite关系表达式树
        RelNode relNode = queryOperation.getCalciteTree();
        Tuple2<String, RelNode> operationRelNode = new Tuple2<>(sinkOperation.getContextResolvedTable().getIdentifier().asSummaryString(), relNode);

        List<String> targetColumnList = tableEnv.from(
                operationRelNode.f0.split("\\.")[1] + "." +
                        operationRelNode.f0.split("\\.")[2]).getResolvedSchema().getColumnNames();
        // 获取元数据查询对象，该接口提供了来查询关系代数表达式RelNode的各种元数据属性
        RelMetadataQuery metadataQuery = relNode.getCluster().getMetadataQuery();
        ArrayList<LineageColumnRel> resultList = new ArrayList<>();
        for (int index = 0; index < targetColumnList.size(); index++) {
            String targetColumn = targetColumnList.get(index);
            // metadataQuery.getColumnOrigins可以确定查询结果中每一列的来源
            // 例如metadataQuery.getColumnOrigins(relNode,0):返回目标表第0列(id)的来源信息
            // 针对此SQL，RelColumnOrigin对象指向源表test.student_back表的第0列(id)
            Set<RelColumnOrigin> relColumnOriginSet = metadataQuery.getColumnOrigins(relNode, index);
            for (RelColumnOrigin relColumnOrigin : relColumnOriginSet) {
                RelOptTable table = relColumnOrigin.getOriginTable();
                String sourceTable = String.join(".", table.getQualifiedName());
                int ordinal = relColumnOrigin.getOriginColumnOrdinal();
                TableSourceTable tableSourceTable = (TableSourceTable) table;
                List<String> fieldNames = tableSourceTable.contextResolvedTable().getResolvedSchema().getColumnNames();
                String sourceColumn = fieldNames.get(ordinal);
                resultList.add(LineageConvert.buildLineageColumnRel(sourceTable, sourceColumn, operationRelNode.f0, targetColumn));
            }
        }
        System.out.println(resultList);

        env.execute();
    }
}
