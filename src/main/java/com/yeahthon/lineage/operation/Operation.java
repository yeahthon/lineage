package com.yeahthon.lineage.operation;

// Operation 是解析SQL得到的中间表示IR，比SQL语句低一层，比物理执行计划高一层
public interface Operation extends org.apache.flink.table.operations.Operation {
    String getHandle();

    // 工厂方法设计，接口本身定义了“如何根据SQL创建自己”的约定
    Operation create(String statement);

    @Override
    default String asSummaryString() {
        return getHandle();
    }

}
