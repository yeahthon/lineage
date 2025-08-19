package com.yeahthon.lineage.bean;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class LineageResult {
    private List<LineageTable> tables = new ArrayList<>();
    private List<LineageRelation> relations = new ArrayList<>();
}
