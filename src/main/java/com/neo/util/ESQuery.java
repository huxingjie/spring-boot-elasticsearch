package com.neo.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 配置ES查询注解
 */
@Retention(value = RetentionPolicy.RUNTIME)
@Target(value = ElementType.FIELD)
public @interface ESQuery {
    /**
     * 配置ES document属性名
     * 不配置则默认字段名
     */
    String key() default "";

    /**
     * 包括：
     * term  默认精确匹配字段
     * terms 多值查询
     * rangeStart, rangeEnd 范围查询
     * match 分词查询
     */
    ESQueryType type() default ESQueryType.TERM;

    /**
     * type为terms和range时, 对值进行拆分
     * 默认
     */
    String split() default ",";
}

