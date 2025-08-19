package com.yeahthon.lineage.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class FunctionResult {
    private String catalogName;
    private String database;
    private String functionName;

    public static Set<FunctionResult> buildResult(String catalog,
                                                  String database,
                                                  String[] expectedArray) {
        return Stream.of(expectedArray)
                .map(element -> new FunctionResult(catalog, database, element))
                .collect(Collectors.toSet());
    }
}
