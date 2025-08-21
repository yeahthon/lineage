package com.yeahthon.lineage.base;

import com.yeahthon.lineage.bean.LineageColumnRel;
import com.yeahthon.lineage.bean.LineageRelation;
import com.yeahthon.lineage.bean.LineageResult;
import com.yeahthon.lineage.bean.LineageTable;

import java.util.List;

public class LineageConvert {
    public static LineageColumnRel buildLineageColumnRel(
            String sourceTablePath,
            String sourceColumn,
            String targetTablePath,
            String targetColumn) {
        String[] sourceItems = sourceTablePath.split("\\.");
        String[] targetItems = targetTablePath.split("\\.");
        LineageColumnRel lineageColumnRel = LineageColumnRel.builder().build();
        lineageColumnRel.setSourceTablePath(sourceTablePath);
        lineageColumnRel.setTargetTablePath(targetTablePath);
        lineageColumnRel.setSourceCatalog(sourceItems[0]);
        lineageColumnRel.setSourceDatabase(sourceItems[1]);
        lineageColumnRel.setSourceTable(sourceItems[2]);
        lineageColumnRel.setSourceColumn(sourceColumn);
        lineageColumnRel.setTargetCatalog(targetItems[0]);
        lineageColumnRel.setTargetDatabase(targetItems[1]);
        lineageColumnRel.setTargetTable(targetItems[2]);
        lineageColumnRel.setTargetColumn(targetColumn);
        return lineageColumnRel;
    }

    public static LineageTable buildLineageTable(
            String id,
            String name) {
        String[] dbs = name.split("\\.");
        LineageTable lineageTable = LineageTable.builder().build();
        lineageTable.setId(id);
        lineageTable.setName(name);
        lineageTable.setDbName(dbs[1]);
        lineageTable.setTableName(dbs[2]);
        return lineageTable;
    }

    /***/
    public static LineageRelation buildLineageRelation(
            //String id,
            String srcTableId,
            String tarTableId,
            String srcTableColName,
            String tarTableColName) {
        LineageRelation lineageColumnRel = LineageRelation.builder().build();
        //.setId(id)
        lineageColumnRel.setSrcTableId(srcTableId);
        lineageColumnRel.setTarTableId(tarTableId);
        lineageColumnRel.setSrcTableColName(srcTableColName);
        lineageColumnRel.setTarTableColName(tarTableColName);
        return lineageColumnRel;
    }

    public static LineageResult buildLineageResult(
            List<LineageTable> tables,
            List<LineageRelation> relations) {
        LineageResult lineageResult = new LineageResult();
        lineageResult.setTables(tables);
        lineageResult.setRelations(relations);
        return lineageResult;
    }
}
