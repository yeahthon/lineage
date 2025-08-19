package com.yeahthon.lineage.bean;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Data
@Builder
@Accessors(chain = true)
public class LineageTable implements Serializable {
    private String id;
    private String name;
    private String dbName;
    private String tableName;
}
