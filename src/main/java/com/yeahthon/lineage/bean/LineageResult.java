package com.yeahthon.lineage.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class LineageResult {
    private List<LineageTable> tables = new ArrayList<>();
    private List<LineageRelation> relations = new ArrayList<>();
}
