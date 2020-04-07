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

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.KeySequence;
import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.toolkit.*;
import com.baomidou.mybatisplus.core.toolkit.sql.SqlScriptUtils;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.ibatis.mapping.ResultFlag;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.ResultMapping;
import org.apache.ibatis.session.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static java.util.stream.Collectors.joining;

/**
 * 数据库表反射信息
 *
 * @author hubin
 * @since 2016-01-23
 */
@Data
@Setter(AccessLevel.PACKAGE)
@Accessors(chain = true)
public class TableInfo implements Constants {

    /**
     * 实体类型
     */
    private Class<?> entityType;
    /**
     * 表主键ID 类型
     */
    private IdType idType = IdType.NONE;
    /**
     * 表名称
     */
    private String tableName;
    /**
     * 表映射结果集
     */
    private String resultMap;
    /**
     * 是否是需要自动生成的 resultMap
     */
    private boolean autoInitResultMap;
    /**
     * 是否是自动生成的 resultMap
     */
    private boolean initResultMap;
    /**
     * 主键是否有存在字段名与属性名关联
     * <p>true: 表示要进行 as</p>
     */
    private boolean keyRelated;
    /**
     * 表主键ID 字段名（指数据库中的字段名，eg: user_id）
     */
    private String keyColumn;
    /**
     * 表主键ID 属性名（指pojo对象的属性名称，eg: userId）
     */
    private String keyProperty;
    /**
     * 表主键ID 属性类型(pojo 对象属性的类型 eg: String/Long/Boolean...)
     */
    private Class<?> keyType;
    /**
     * 表主键ID Sequence
     */
    private KeySequence keySequence;
    /**
     * 表字段信息列表
     */
    private List<TableFieldInfo> fieldList;
    /**
     * 命名空间 (对应的 mapper 接口的全类名)
     */
    private String currentNamespace;
    /**
     * MybatisConfiguration 标记 (Configuration内存地址值)
     */
    @Getter
    private MybatisConfiguration configuration;
    /**
     * 是否开启逻辑删除
     */
    @Setter(AccessLevel.NONE)
    private boolean logicDelete;
    /**
     * 是否开启下划线转驼峰
     */
    private boolean underCamel;
    /**
     * 缓存包含主键及字段的 sql select
     */
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private String allSqlSelect;
    /**
     * 缓存主键字段的 sql select
     */
    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private String sqlSelect;
    /**
     * 表字段是否启用了插入填充
     *
     * @since 3.3.0
     */
    @Getter
    @Setter(AccessLevel.NONE)
    private boolean withInsertFill;
    /**
     * 表字段是否启用了更新填充
     *
     * @since 3.3.0
     */
    @Getter
    @Setter(AccessLevel.NONE)
    private boolean withUpdateFill;
    /**
     * 表字段是否启用了乐观锁
     *
     * @since 3.3.1
     */
    @Getter
    @Setter(AccessLevel.NONE)
    private boolean withVersion;
    /**
     * 乐观锁字段
     *
     * @since 3.3.1
     */
    @Getter
    @Setter(AccessLevel.NONE)
    private TableFieldInfo versionFieldInfo;

    public TableInfo(Class<?> entityType) {
        this.entityType = entityType;
    }

    /**
     * 获得注入的 SQL Statement
     *
     * @param sqlMethod MybatisPlus 支持 SQL 方法
     * @return SQL Statement
     */
    public String getSqlStatement(String sqlMethod) {
        return currentNamespace + DOT + sqlMethod;
    }

    /**
     * 设置 Configuration
     */
    void setConfiguration(Configuration configuration) {
        Assert.notNull(configuration, "Error: You need Initialize MybatisConfiguration !");
        this.configuration = (MybatisConfiguration) configuration;
        this.underCamel = configuration.isMapUnderscoreToCamelCase();
    }

    /**
     * 是否有主键
     *
     * @return 是否有
     */
    public boolean havePK() {
        return StringUtils.isNotBlank(keyColumn);
    }

    /**
     * 获取主键的 select sql 片段
     * 主键字段 eg：id
     *
     * @return sql 片段
     */
    public String getKeySqlSelect() {
        if (sqlSelect != null) {
            return sqlSelect;
        }
        if (havePK()) {
            sqlSelect = keyColumn;
            if (keyRelated) {
                sqlSelect += (" AS " + keyProperty);
            }
        } else {
            sqlSelect = EMPTY;
        }
        return sqlSelect;
    }

    /**
     * 获取包含主键及字段的 select sql 片段
     * eg：id, name, sex ...
     *
     * @return sql 片段
     */
    public String getAllSqlSelect() {
        if (allSqlSelect != null) {
            return allSqlSelect;
        }
        // 查询字段筛选
        allSqlSelect = chooseSelect(TableFieldInfo::isSelect);
        return allSqlSelect;
    }

    /**
     * 获取需要进行查询的 select sql 片段
     * <p>在不为空的前提下，返回优先级如下：</p>
     * <p>优先级：主键和查询字段 > 查询字段 > 主键</p>
     *
     * @param predicate 过滤条件
     * @return sql 片段
     */
    public String chooseSelect(Predicate<TableFieldInfo> predicate) {
        // 拿到主键字段 eg：id
        String sqlSelect = getKeySqlSelect();
        // 根据过滤条件筛选出需要查询的字段，并用逗号分隔
        String fieldsSqlSelect = fieldList.stream().filter(predicate)
            .map(TableFieldInfo::getSqlSelect).collect(joining(COMMA));

        if (StringUtils.isNotBlank(sqlSelect) && StringUtils.isNotBlank(fieldsSqlSelect)) {
            // 拿到的主键和字段全不为空，逗号分隔并返回
            return sqlSelect + COMMA + fieldsSqlSelect;
        } else if (StringUtils.isNotBlank(fieldsSqlSelect)) {
            // 如查询字段不为空，返回拿到的查询字段
            return fieldsSqlSelect;
        }
        return sqlSelect;
    }

    /**
     * 获取 insert 时候主键 sql 脚本片段
     * <p>insert into table (字段) values (值)</p>
     * <p>位于 "值" 部位</p>
     * <P>这里的 "值" 并非具体的值，而是例如xml中的填充值得变量 eg： #{userId}</P>
     *
     * @return sql 脚本片段
     */
    public String getKeyInsertSqlProperty(final String prefix, final boolean newLine) {
        // 判断是否有字段前缀
        final String newPrefix = prefix == null ? EMPTY : prefix;
        if (havePK()) {
            if (idType == IdType.AUTO) {
                // 当主键为自增时直接返回空
                return EMPTY;
            }
            // 返回安全入参sql判断 eg： #{id}
            return SqlScriptUtils.safeParam(newPrefix + keyProperty) + COMMA + (newLine ? NEWLINE : EMPTY);
        }
        return EMPTY;
    }

    /**
     * 获取 insert 时候主键 sql 脚本片段
     * <p>insert into table (字段) values (值)</p>
     * <p>位于 "字段" 部位</p>
     * eg: id
     *
     * @return sql 脚本片段
     */
    public String getKeyInsertSqlColumn(final boolean newLine) {
        if (havePK()) {
            if (idType == IdType.AUTO) {
                // 自增，主键保持为空
                return EMPTY;
            }
            // eg: id
            return keyColumn + COMMA + (newLine ? NEWLINE : EMPTY);
        }
        return EMPTY;
    }

    /**
     * 获取所有 insert 时候插入值 sql 脚本片段
     * <p>insert into table (字段) values (值)</p>
     * <p>位于 "值" 部位</p>
     *
     * <li> 自动选部位,根据规则会生成 if 标签 </li>
     *
     * @return sql 脚本片段
     * eg:
     * <pre>
     *  #{id},#{name} or
     *  #{id},<if test="name != null and name != ''">#{name}</if>
     * </pre>
     */
    public String getAllInsertSqlPropertyMaybeIf(final String prefix) {
        final String newPrefix = prefix == null ? EMPTY : prefix;
        // getKeyInsertSqlProperty 获取主键的sql片段，eg: #{id}
        return getKeyInsertSqlProperty(newPrefix, true) + fieldList.stream()
            // getInsertSqlPropertyMaybeIf 获取插入“值”的sql片段
            // eg: #{name} or <if test="name != null and name != ''">#{name}</if>
            .map(i -> i.getInsertSqlPropertyMaybeIf(newPrefix)).filter(Objects::nonNull)
            .collect(joining(NEWLINE));
    }

    /**
     * 获取 insert 时候字段 sql 脚本片段
     * <p>insert into table (字段) values (值)</p>
     * <p>位于 "字段" 部位</p>
     *
     * <li> 自动选部位,根据规则会生成 if 标签 </li>
     *
     * @return sql 脚本片段
     */
    public String getAllInsertSqlColumnMaybeIf() {
        // getKeyInsertSqlColumn 获取主键字段
        return getKeyInsertSqlColumn(true) + fieldList.stream()
            // 对字段进行处理，对需要添加<if/>标签的字段增加标签
            .map(TableFieldInfo::getInsertSqlColumnMaybeIf)
            .filter(Objects::nonNull).collect(joining(NEWLINE));
    }

    /**
     * 获取所有的查询的 sql where 片段
     *
     * @param ignoreLogicDelFiled 是否过滤掉逻辑删除字段
     * @param withId              是否包含 id 项
     * @param prefix              前缀
     * @return sql 脚本片段
     */
    public String getAllSqlWhere(boolean ignoreLogicDelFiled, boolean withId, final String prefix) {
        final String newPrefix = prefix == null ? EMPTY : prefix;
        String filedSqlScript = fieldList.stream()
            .filter(i -> {
                // 过滤逻辑删除字段
                if (ignoreLogicDelFiled) {
                    // ! 是否开启逻辑删除 && 当前字段是否启用逻辑删除
                    // 启用逻辑删除，返回false，过滤掉当前字段
                    return !(isLogicDelete() && i.isLogicDelete());
                }
                return true;
            })
            // 拿到where条件
            .map(i -> i.getSqlWhere(newPrefix)).filter(Objects::nonNull).collect(joining(NEWLINE));

        if (!withId || StringUtils.isBlank(keyProperty)) {
            // 不包含主键 或 主键属性为空
            return filedSqlScript;
        }
        String newKeyProperty = newPrefix + keyProperty;
        // 拿到主键 eg： id = #{id}
        String keySqlScript = keyColumn + EQUALS + SqlScriptUtils.safeParam(newKeyProperty);
        // <if> 标签包裹
        return SqlScriptUtils.convertIf(keySqlScript, String.format("%s != null", newKeyProperty), false)
            + NEWLINE + filedSqlScript;
    }

    /**
     * 获取所有的 sql set 片段
     *
     * @param ignoreLogicDelFiled 是否过滤掉逻辑删除字段
     * @param prefix              前缀
     * @return sql 脚本片段
     */
    public String getAllSqlSet(boolean ignoreLogicDelFiled, final String prefix) {
        final String newPrefix = prefix == null ? EMPTY : prefix;
        return fieldList.stream()
            .filter(i -> {
                if (ignoreLogicDelFiled) {
                    return !(isLogicDelete() && i.isLogicDelete());
                }
                return true;
            }).map(i -> i.getSqlSet(newPrefix)).filter(Objects::nonNull).collect(joining(NEWLINE));
    }

    /**
     * 获取逻辑删除字段的 sql 脚本
     *
     * @param startWithAnd 是否以 and 开头
     * @param isWhere      是否需要的是逻辑删除值
     * @return sql 脚本
     */
    public String getLogicDeleteSql(boolean startWithAnd, boolean isWhere) {
        if (logicDelete) {
            TableFieldInfo field = fieldList.stream().filter(TableFieldInfo::isLogicDelete).findFirst()
                .orElseThrow(() -> ExceptionUtils.mpe("can't find the logicFiled from table {%s}", tableName));
            String logicDeleteSql = formatLogicDeleteSql(field, isWhere);
            if (startWithAnd) {
                logicDeleteSql = " AND " + logicDeleteSql;
            }
            return logicDeleteSql;
        }
        return EMPTY;
    }

    /**
     * format logic delete SQL, can be overrided by subclass
     * github #1386
     *
     * @param field   TableFieldInfo
     * @param isWhere true: logicDeleteValue, false: logicNotDeleteValue
     * @return
     */
    private String formatLogicDeleteSql(TableFieldInfo field, boolean isWhere) {
        final String value = isWhere ? field.getLogicNotDeleteValue() : field.getLogicDeleteValue();
        if (isWhere) {
            if (NULL.equalsIgnoreCase(value)) {
                return field.getColumn() + " IS NULL";
            } else {
                return field.getColumn() + EQUALS + String.format(field.isCharSequence() ? "'%s'" : "%s", value);
            }
        }
        final String targetStr = field.getColumn() + EQUALS;
        if (NULL.equalsIgnoreCase(value)) {
            return targetStr + NULL;
        } else {
            return targetStr + String.format(field.isCharSequence() ? "'%s'" : "%s", value);
        }
    }

    /**
     * 自动构建 resultMap 并注入(如果条件符合的话)
     */
    void initResultMapIfNeed() {
        if (autoInitResultMap && null == resultMap) {
            String id = currentNamespace + DOT + MYBATIS_PLUS + UNDERSCORE + entityType.getSimpleName();
            List<ResultMapping> resultMappings = new ArrayList<>();
            if (havePK()) {
                ResultMapping idMapping = new ResultMapping.Builder(configuration, keyProperty, keyColumn, keyType)
                    .flags(Collections.singletonList(ResultFlag.ID)).build();
                resultMappings.add(idMapping);
            }
            if (CollectionUtils.isNotEmpty(fieldList)) {
                fieldList.forEach(i -> resultMappings.add(i.getResultMapping(configuration)));
            }
            ResultMap resultMap = new ResultMap.Builder(configuration, id, entityType, resultMappings).build();
            configuration.addResultMap(resultMap);
            this.resultMap = id;
        }
    }

    void setFieldList(List<TableFieldInfo> fieldList) {
        this.fieldList = fieldList;
        fieldList.forEach(i -> {
            if (i.isLogicDelete()) {
                this.logicDelete = true;
            }
            if (i.isWithInsertFill()) {
                this.withInsertFill = true;
            }
            if (i.isWithUpdateFill()) {
                this.withUpdateFill = true;
            }
            if (i.isVersion()) {
                this.withVersion = true;
                this.versionFieldInfo = i;
            }
        });
    }
}
