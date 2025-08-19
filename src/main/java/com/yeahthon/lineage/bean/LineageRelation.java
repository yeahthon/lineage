package com.yeahthon.lineage.bean;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Data
@Builder
@Accessors(chain = true)
public class LineageRelation implements Serializable {
    private String id;
    private String srcTableId;
    private String tarTableId;
    private String srcTableColName;
    private String tarTableColName;
}
