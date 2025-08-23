package com.yeahthon.lineage.operation;

import com.yeahthon.lineage.enums.SqlTypeEnum;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

@Slf4j
@NoArgsConstructor
public class Operations {
    public static final String SQL_EMPTY_STR = "[\\s\\t\\n\\r]";

    private static final Operation[] ALL_OPERATIONS = getAllOperations();

    private static Operation[] getAllOperations() {
        // 使用 Reflections 获取所有 Operations 的子类或者实现类
        Reflections reflections = new Reflections(Operation.class.getPackage().getName());
        // Scanners.SubTypes.of(Operations.class).asClass() 表示查找所有 Operations 的子类型
        Set<Class<?>> operations = reflections.get(Scanners.SubTypes.of(Operations.class).asClass());

        return operations.stream()
                // 过滤掉接口类型，只保留具体类
                .filter(clz -> !clz.isInterface())
                .map(clz -> {
                    try {
                        // 将每个 Class 对象映射为其实例
                        return clz.getConstructor().newInstance();
                    } catch (InstantiationException
                             | IllegalAccessException
                             | InvocationTargetException
                             | NoSuchMethodException e) {
                        // 异常处理包括：无法实例化（如抽象类）、无法访问构造方法、构造方法抛出异常、没有无参构造方法等
                        log.error("getAllOperations error, class: {}, err: {}", clz.getName(), e.getMessage());

                        throw new RuntimeException(e);
                    } catch (NoClassDefFoundError e) {
                        log.warn("getAllOperations error,  If you do not have this class, please add the corresponding dependency. Operation: {}.{}",
                                clz.getName(),
                                clz.getSimpleName());

                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .toArray(Operation[]::new);
    }

    // 获取单个SQL语句的Operation类型（枚举类中的类型）
    public static SqlTypeEnum getOperationType(String sql) {
        // 将SQL转为大写并调整为一行，并将所有的空格、换行（包含tab等）均替换为单个空格
        String sqlTrim = sql.replaceAll(SQL_EMPTY_STR, " ").trim().toUpperCase();

        return Arrays.stream(SqlTypeEnum.values())
                // 判断SQL语句是否符合枚举类中的一种
                .filter(sqlType -> sqlType.match(sqlTrim))
                .findFirst()
                .orElse(SqlTypeEnum.UNKNOWN);
    }
}
