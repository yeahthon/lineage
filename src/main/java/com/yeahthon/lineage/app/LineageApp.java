package com.yeahthon.lineage.app;

import com.yeahthon.lineage.base.LineageConvert;
import com.yeahthon.lineage.bean.LineageColumnRel;
import com.yeahthon.lineage.bean.LineageResult;
import com.yeahthon.lineage.enums.SqlTypeEnum;
import com.yeahthon.lineage.operation.Operations;
import com.yeahthon.lineage.utils.SQLUtil;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
            SqlTypeEnum operationType = Operations.getOperationType(sql);
            if (operationType == SqlTypeEnum.INSERT) {
                lineageRelList.addAll(parseSqlLineage(sql));
            } else if (!(operationType == SqlTypeEnum.SELECT) && !(operationType == SqlTypeEnum.PRINT)) {
                tableContext.executeSql(sql);
            }
        }

        return LineageResult.builder().build();
    }

    public List<LineageColumnRel> parseSqlLineage(String sql) {
        Tuple2<String, RelNode> tuple = getOperation(sql);

        return parseColumnRelation(tuple.f0, tuple.f1);
    }

    // 提取操作类型Operation和关系表达式树RelNode
    public Tuple2<String, RelNode> getOperation(String sql) {
        List<Operation> operations = tableEnv.getParser().parse(sql);
        if (operations.size() != 1) {
            throw new TableException("SQL query are not supported! Please enter a single SQL statement!");
        } else if (operations.get(0) instanceof SinkModifyOperation) {
            Operation operation = operations.get(0);
            SinkModifyOperation sinkOperation = (SinkModifyOperation) operation;
            PlannerQueryOperation queryOperation = (PlannerQueryOperation) sinkOperation.getChild();
            RelNode relNode = queryOperation.getCalciteTree();
            return new Tuple2<>(sinkOperation.getContextResolvedTable().getIdentifier().asSummaryString(), relNode);
        } else {
            throw new TableException("Only insert is supported now!");
        }
    }

    public List<LineageColumnRel> parseColumnRelation(String targetTable, RelNode relNode) {
        List<String> targetColumnList = tableEnv.from(targetTable).getResolvedSchema().getColumnNames();
        List<String> queryFieldList = relNode.getRowType().getFieldNames();

        if (queryFieldList.size() != targetColumnList.size()) {
            throw new ValidationException(String.format(
                    "Column types of query result and sink for %s do not match.\n" +
                            "Query schema: %s" +
                            "Sink schema: %s",
                    targetTable,
                    queryFieldList,
                    targetColumnList));
        } else {
            RelMetadataQuery metadataQuery = relNode.getCluster().getMetadataQuery();
            ArrayList<LineageColumnRel> resultList = new ArrayList<>();
            for (int index = 0; index < targetColumnList.size(); index++) {
                String targetColumn = targetColumnList.get(index);
                Set<RelColumnOrigin> relColumnOriginSet = metadataQuery.getColumnOrigins(relNode, index);
                for (RelColumnOrigin relColumnOrigin : relColumnOriginSet) {
                    RelOptTable table = relColumnOrigin.getOriginTable();
                    String sourceTable = String.join(".", table.getQualifiedName());
                    int ordinal = relColumnOrigin.getOriginColumnOrdinal();
                    TableSourceTable tableSourceTable = (TableSourceTable) table;
                    List<String> fieldNames = tableSourceTable.contextResolvedTable().getResolvedSchema().getColumnNames();
                    String sourceColumn = fieldNames.get(ordinal);
                    resultList.add(LineageConvert.buildLineageColumnRel(sourceTable, sourceColumn, targetTable, targetColumn));
                }
            }
            return resultList;
        }
    }
}
