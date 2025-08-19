package com.yeahthon.lineage.bean;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class LineageColumnRel implements Serializable {
    private String sourceTablePath;

    private String targetTablePath;

    private String sourceCatalog;

    private String sourceDatabase;

    private String sourceTable;

    private String sourceColumn;

    private String targetCatalog;

    private String targetDatabase;

    private String targetTable;

    private String targetColumn;
}
