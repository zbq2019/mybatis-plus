/*
 * Copyright (c) 2011-2020, baomidou (jobob@qq.com).
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.baomidou.mybatisplus.core.metadata;

import com.baomidou.mybatisplus.annotation.*;
import com.baomidou.mybatisplus.core.config.GlobalConfig;
import com.baomidou.mybatisplus.core.incrementer.IKeyGenerator;
import com.baomidou.mybatisplus.core.toolkit.*;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.apache.ibatis.builder.StaticSqlSource;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.keygen.SelectKeyGenerator;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.reflection.Reflector;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.session.Configuration;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.stream.Collectors.toList;

/**
 * <p>
 * 实体类反射表辅助类
 * </p>
 *
 * @author hubin sjy
 * @since 2016-09-09
 */
public class TableInfoHelper {

    private static final Log logger = LogFactory.getLog(TableInfoHelper.class);

    /**
     * 储存反射类表信息
     */
    private static final Map<Class<?>, TableInfo> TABLE_INFO_CACHE = new ConcurrentHashMap<>();

    /**
     * 默认表主键名称
     */
    private static final String DEFAULT_ID_NAME = "id";

    /**
     * <p>
     * 获取实体映射表信息
     * </p>
     *
     * @param clazz 反射实体类
     * @return 数据库表反射信息
     */
    public static TableInfo getTableInfo(Class<?> clazz) {
        if (clazz == null
            // true：基本类型或包装类型
            || ReflectionKit.isPrimitiveOrWrapper(clazz)
            || clazz == String.class) {
            return null;
        }
        // https://github.com/baomidou/mybatis-plus/issues/299
        TableInfo tableInfo = TABLE_INFO_CACHE.get(ClassUtils.getUserClass(clazz));
        if (null != tableInfo) {
            return tableInfo;
        }
        //尝试获取父类缓存
        Class<?> currentClass = clazz;
        while (null == tableInfo && Object.class != currentClass) {
            currentClass = currentClass.getSuperclass();
            tableInfo = TABLE_INFO_CACHE.get(ClassUtils.getUserClass(currentClass));
        }
        if (tableInfo != null) {
            TABLE_INFO_CACHE.put(ClassUtils.getUserClass(clazz), tableInfo);
        }
        return tableInfo;
    }

    /**
     * <p>
     * 获取所有实体映射表信息
     * </p>
     *
     * @return 数据库表反射信息集合
     */
    @SuppressWarnings("unused")
    public static List<TableInfo> getTableInfos() {
        return new ArrayList<>(TABLE_INFO_CACHE.values());
    }

    /**
     * <p>
     * 实体类反射获取表信息【初始化】
     * </p>
     * MapperBuilderAssistant用于缓存、sql参数、查询返回的结果集处理
     *
     * @param clazz 反射实体类
     * @return 数据库表反射信息
     */
    public synchronized static TableInfo initTableInfo(MapperBuilderAssistant builderAssistant, Class<?> clazz) {
        TableInfo tableInfo = TABLE_INFO_CACHE.get(clazz);
        if (tableInfo != null) {
            if (builderAssistant != null) {
                tableInfo.setConfiguration(builderAssistant.getConfiguration());
            }
            return tableInfo;
        }

        /* 没有获取到缓存信息,则初始化 */
        tableInfo = new TableInfo(clazz);
        GlobalConfig globalConfig;
        if (null != builderAssistant) {
            // 命名空间
            tableInfo.setCurrentNamespace(builderAssistant.getCurrentNamespace());
            // config
            tableInfo.setConfiguration(builderAssistant.getConfiguration());
            // config -> globalConfig
            globalConfig = GlobalConfigUtils.getGlobalConfig(builderAssistant.getConfiguration());
        } else {
            // 兼容测试场景
            globalConfig = GlobalConfigUtils.defaults();
        }

        /* 初始化表名相关 */
        // initTableName 初始化表，返回excludeProperty （需要排除的属性名）
        final String[] excludeProperty = initTableName(clazz, globalConfig, tableInfo);

        // 将需要排除的表字段转为list
        List<String> excludePropertyList = excludeProperty != null && excludeProperty.length > 0 ? Arrays.asList(excludeProperty) : Collections.emptyList();

        /* 初始化字段相关 */
        initTableFields(clazz, globalConfig, tableInfo, excludePropertyList);

        /* 放入缓存 */
        TABLE_INFO_CACHE.put(clazz, tableInfo);

        /* 缓存 lambda */
        // 主键和字段做缓冲
        LambdaUtils.installCache(tableInfo);

        /* 自动构建 resultMap */
        tableInfo.initResultMapIfNeed();

        return tableInfo;
    }

    /**
     * <p>
     * 初始化 表数据库类型,表名,resultMap
     * </p>
     * 优先级： 注解>全局，即细粒度优先
     *
     * @param clazz        实体类
     * @param globalConfig 全局配置
     * @param tableInfo    数据库表反射信息
     * @return 需要排除的字段名
     */
    private static String[] initTableName(Class<?> clazz, GlobalConfig globalConfig, TableInfo tableInfo) {
        /* 数据库全局配置 */
        GlobalConfig.DbConfig dbConfig = globalConfig.getDbConfig();
        TableName table = clazz.getAnnotation(TableName.class);

        String tableName = clazz.getSimpleName();
        // 表前缀
        String tablePrefix = dbConfig.getTablePrefix();
        String schema = dbConfig.getSchema();
        // 使用表前缀
        boolean tablePrefixEffect = true;
        String[] excludeProperty = null;

        if (table != null) {

            // 初始化表名的两种方式
            if (StringUtils.isNotBlank(table.value())) {
                // 注解指定表名
                tableName = table.value();
                // 全局配置表前缀不为空 && table.keepGlobalPrefix() = false（不使用表前缀）
                if (StringUtils.isNotBlank(tablePrefix) && !table.keepGlobalPrefix()) {
                    tablePrefixEffect = false;
                }
            } else {
                // dbConfig 生成表名
                tableName = initTableNameWithDbConfig(tableName, dbConfig);
            }

            if (StringUtils.isNotBlank(table.schema())) {
                schema = table.schema();
            }

            /* 表结果集映射 */
            if (StringUtils.isNotBlank(table.resultMap())) {
                tableInfo.setResultMap(table.resultMap());
            }
            tableInfo.setAutoInitResultMap(table.autoResultMap());
            // 需要排除的属性名
            excludeProperty = table.excludeProperty();

        } else {
            // @TableName 注解不存在,直接根据配置初始化表名
            tableName = initTableNameWithDbConfig(tableName, dbConfig);
        }

        String targetTableName = tableName;
        // 表前缀不为空 && 自动补充前缀=true
        if (StringUtils.isNotBlank(tablePrefix) && tablePrefixEffect) {
            // 拼接表前缀
            targetTableName = tablePrefix + targetTableName;
        }

        if (StringUtils.isNotBlank(schema)) {
            // schema 不为空，目标表名 = schema.表名称
            targetTableName = schema + StringPool.DOT + targetTableName;
        }

        tableInfo.setTableName(targetTableName);

        /* 开启了自定义 KEY 生成器 */
        if (null != dbConfig.getKeyGenerator()) {
            tableInfo.setKeySequence(clazz.getAnnotation(KeySequence.class));
        }
        return excludeProperty;
    }

    /**
     * 根据 DbConfig 初始化 表名
     *
     * @param className 类名
     * @param dbConfig  DbConfig
     * @return 表名
     */
    private static String initTableNameWithDbConfig(String className, GlobalConfig.DbConfig dbConfig) {
        String tableName = className;
        // 开启表名下划线申明
        if (dbConfig.isTableUnderline()) {
            // 驼峰转下划线
            tableName = StringUtils.camelToUnderline(tableName);
        }
        // 大写命名判断
        if (dbConfig.isCapitalMode()) {
            tableName = tableName.toUpperCase();
        } else {
            // 首字母小写
            tableName = StringUtils.firstToLowerCase(tableName);
        }
        return tableName;
    }

    /**
     * <p>
     * 初始化 表主键,表字段
     * </p>
     *
     * @param clazz        实体类
     * @param globalConfig 全局配置
     * @param tableInfo    数据库表反射信息
     */
    public static void initTableFields(Class<?> clazz, GlobalConfig globalConfig, TableInfo tableInfo, List<String> excludeProperty) {
        /* 数据库全局配置 */
        GlobalConfig.DbConfig dbConfig = globalConfig.getDbConfig();
        ReflectorFactory reflectorFactory = tableInfo.getConfiguration().getReflectorFactory();
        //TODO @咩咩 有空一起来撸完这反射模块.
        Reflector reflector = reflectorFactory.findForClass(clazz);
        List<Field> list = getAllFields(clazz);
        // 标记是否读取到主键
        boolean isReadPK = false;
        // 是否存在 @TableId 注解
        boolean existTableId = isExistTableId(list);

        List<TableFieldInfo> fieldList = new ArrayList<>();
        for (Field field : list) {
            if (excludeProperty.contains(field.getName())) {
                continue;
            }

            /* 主键ID 初始化 */
            if (existTableId) {
                // 存在主键
                TableId tableId = field.getAnnotation(TableId.class);
                if (tableId != null) {
                    if (isReadPK) {
                        // isReadPK,默认false，如果为true，说明已经存在一个主键
                        throw ExceptionUtils.mpe("@TableId can't more than one in Class: \"%s\".", clazz.getName());
                    } else {
                        // 初始化主键，初始化成功返回true
                        isReadPK = initTableIdWithAnnotation(dbConfig, tableInfo, field, tableId, reflector);
                        continue;
                    }
                }

            } else if (!isReadPK) {
                // 再度按照默认主键为“id”进行初始化，成功true，否则false
                isReadPK = initTableIdWithoutAnnotation(dbConfig, tableInfo, field, reflector);
                if (isReadPK) {
                    continue;
                }
            }

            /* 有 @TableField 注解的字段初始化 */
            if (initTableFieldWithAnnotation(dbConfig, tableInfo, fieldList, field)) {
                continue;
            }

            /* 无 @TableField 注解的字段初始化 */
            fieldList.add(new TableFieldInfo(dbConfig, tableInfo, field));
        }

        /* 检查逻辑删除字段只能有最多一个 */
        Assert.isTrue(fieldList.parallelStream().filter(TableFieldInfo::isLogicDelete).count() < 2L,
            String.format("@TableLogic can't more than one in Class: \"%s\".", clazz.getName()));

        /* 字段列表,不可变集合 */
        tableInfo.setFieldList(Collections.unmodifiableList(fieldList));

        /* 未发现主键注解，提示警告信息 */
        if (!isReadPK) {
            logger.warn(String.format("Can not find table primary key in Class: \"%s\".", clazz.getName()));
        }
    }

    /**
     * <p>
     * 判断主键注解是否存在
     * </p>
     *
     * @param list 字段列表
     * @return true 为存在 @TableId 注解;
     */
    public static boolean isExistTableId(List<Field> list) {
        return list.stream().anyMatch(field -> field.isAnnotationPresent(TableId.class));
    }

    /**
     * <p>
     * 主键属性初始化
     * </p>
     *
     * @param dbConfig  全局配置信息
     * @param tableInfo 表信息
     * @param field     字段
     * @param tableId   注解
     * @param reflector Reflector
     */
    private static boolean initTableIdWithAnnotation(GlobalConfig.DbConfig dbConfig, TableInfo tableInfo,
                                                     Field field, TableId tableId, Reflector reflector) {
        boolean underCamel = tableInfo.isUnderCamel();
        final String property = field.getName();
        if (field.getAnnotation(TableField.class) != null) {
            // 既使用了 @TableId，又使用了@TableField，@TableField将不起作用
            logger.warn(String.format("This \"%s\" is the table primary key by @TableId annotation in Class: \"%s\",So @TableField annotation will not work!",
                property, tableInfo.getEntityType().getName()));
        }
        /* 主键策略（ 注解 > 全局 ） */
        // 设置 Sequence 其他策略无效
        if (IdType.NONE == tableId.type()) {
            // 未设置，使用全局策略
            tableInfo.setIdType(dbConfig.getIdType());
        } else {
            tableInfo.setIdType(tableId.type());
        }

        /* 字段 */
        String column = property;
        if (StringUtils.isNotBlank(tableId.value())) {
            // 当注解value不为空时，使用注解的value
            column = tableId.value();

        } else {
            // Java属性名按配置转换为数据库字段名
            // 开启字段下划线申明
            if (underCamel) {
                column = StringUtils.camelToUnderline(column);
            }
            // 全局大写命名
            if (dbConfig.isCapitalMode()) {
                column = column.toUpperCase();
            }
        }

        // checkRelated(underCamel, property, column) 当属性与数据库字段一致时，返回false，即查询时不需要使用as
        tableInfo.setKeyRelated(checkRelated(underCamel, property, column))
            .setKeyColumn(column)
            .setKeyProperty(property)
            .setKeyType(reflector.getGetterType(property));
        return true;
    }

    /**
     * <p>
     * 主键属性初始化
     * </p>
     *
     * @param tableInfo 表信息
     * @param field     字段
     * @param reflector Reflector
     * @return true 继续下一个属性判断，返回 continue;
     */
    private static boolean initTableIdWithoutAnnotation(GlobalConfig.DbConfig dbConfig, TableInfo tableInfo,
                                                        Field field, Reflector reflector) {
        final String property = field.getName();
        if (DEFAULT_ID_NAME.equalsIgnoreCase(property)) {
            // 一种默认情况
            // 判断主键是否为默认值，即id
            if (field.getAnnotation(TableField.class) != null) {
                //  既使用了 @TableId，又使用了@TableField，@TableField将不起作用
                logger.warn(String.format("This \"%s\" is the table primary key by default name for `id` in Class: \"%s\",So @TableField will not work!",
                    property, tableInfo.getEntityType().getName()));
            }

            // "id"不存在驼峰情况，所以只需要判断是否大写即可
            String column = property;
            if (dbConfig.isCapitalMode()) {
                column = column.toUpperCase();
            }

            // checkRelated(underCamel, property, column) 当属性与数据库字段一致时，返回false，即查询时不需要使用as
            tableInfo.setKeyRelated(checkRelated(tableInfo.isUnderCamel(), property, column))
                .setIdType(dbConfig.getIdType())
                .setKeyColumn(column)
                .setKeyProperty(property)
                .setKeyType(reflector.getGetterType(property));
            return true;
        }

        // 属性不是"id" 返回false
        return false;
    }

    /**
     * <p>
     * 字段属性初始化
     * </p>
     *
     * @param dbConfig  数据库全局配置
     * @param tableInfo 表信息
     * @param fieldList 字段列表
     * @return true 继续下一个属性判断，返回 continue;
     */
    private static boolean initTableFieldWithAnnotation(GlobalConfig.DbConfig dbConfig, TableInfo tableInfo,
                                                        List<TableFieldInfo> fieldList, Field field) {
        /* 获取注解属性，自定义字段 */
        TableField tableField = field.getAnnotation(TableField.class);
        if (null == tableField) {
            return false;
        }
        fieldList.add(new TableFieldInfo(dbConfig, tableInfo, field, tableField));
        return true;
    }

    /**
     * <p>
     * 判定 related 的值
     * </p>
     *
     * @param underCamel 驼峰命名 true 是 ； false 否
     * @param property   属性名
     * @param column     字段名
     * @return related 属性和字段值一致false
     */
    public static boolean checkRelated(boolean underCamel, String property, String column) {
        // 是否符合数据库字段命名
        if (StringUtils.isNotColumnName(column)) {
            // 首尾有转义符,手动在注解里设置了转义符,去除掉转义符
            column = column.substring(1, column.length() - 1);
        }
        String propertyUpper = property.toUpperCase(Locale.ENGLISH);
        String columnUpper = column.toUpperCase(Locale.ENGLISH);
        if (underCamel) {
            // 开启了驼峰并且 column 包含下划线
            return !(propertyUpper.equals(columnUpper) ||
                propertyUpper.equals(columnUpper.replace(StringPool.UNDERSCORE, StringPool.EMPTY)));
        } else {
            // 未开启驼峰,直接判断 property 是否与 column 相同(全大写)
            return !propertyUpper.equals(columnUpper);
        }
    }

    /**
     * <p>
     * 获取该类的所有属性列表
     * </p>
     *
     * @param clazz 反射类
     * @return 属性集合
     */
    public static List<Field> getAllFields(Class<?> clazz) {
        List<Field> fieldList = ReflectionKit.getFieldList(ClassUtils.getUserClass(clazz));
        return fieldList.stream()
            .filter(field -> {
                /* 过滤注解非表字段属性 */
                TableField tableField = field.getAnnotation(TableField.class);
                return (tableField == null || tableField.exist());
            }).collect(toList());
    }

    /**
     * 主键生成
     *
     * @param baseStatementId  /
     * @param tableInfo        /
     * @param builderAssistant /
     * @return /
     */
    public static KeyGenerator genKeyGenerator(String baseStatementId, TableInfo tableInfo, MapperBuilderAssistant builderAssistant) {
        IKeyGenerator keyGenerator = GlobalConfigUtils.getKeyGenerator(builderAssistant.getConfiguration());
        if (null == keyGenerator) {
            throw new IllegalArgumentException("not configure IKeyGenerator implementation class.");
        }
        Configuration configuration = builderAssistant.getConfiguration();
        // builderAssistant.getCurrentNamespace() 当前命名空间
        //TODO 这里不加上builderAssistant.getCurrentNamespace()的会导致com.baomidou.mybatisplus.core.parser.SqlParserHelper.getSqlParserInfo越(chu)界(gui)
        String id = builderAssistant.getCurrentNamespace() + StringPool.DOT + baseStatementId + SelectKeyGenerator.SELECT_KEY_SUFFIX;

        ResultMap resultMap = new ResultMap.Builder(builderAssistant.getConfiguration(), id, tableInfo.getKeyType(), new ArrayList<>()).build();

        MappedStatement mappedStatement = new MappedStatement.Builder(builderAssistant.getConfiguration(), id,
            new StaticSqlSource(configuration, keyGenerator.executeSql(tableInfo.getKeySequence().value()))
            , SqlCommandType.SELECT)
            .keyProperty(tableInfo.getKeyProperty())
            .resultMaps(Collections.singletonList(resultMap))
            .build();
        configuration.addMappedStatement(mappedStatement);
        return new SelectKeyGenerator(mappedStatement, true);
    }
}
