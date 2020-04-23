package com.neo.util;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.lang.reflect.Field;
import java.util.*;
import java.util.Map.Entry;

/**
 * ES查询条件构造
 *
 * @author chenxiaojian
 */
public class ESQueryBuilder {
    private int size = Integer.MAX_VALUE;

    private int from = 0;

    private String asc;

    private String desc;

    private ESCriterion criterion = null;

    //查询条件容器
    private List<ESCriterion> mustCriterions = new ArrayList<>();

    private List<ESCriterion> shouldCriterions = new ArrayList<>();

    private List<ESCriterion> mustNotCriterions = new ArrayList<>();

    /**
     * 构造QueryBuilder
     */
    public QueryBuilder listBuilders() {
        int count = mustCriterions.size() + shouldCriterions.size() + mustNotCriterions.size();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        QueryBuilder queryBuilder = null;
        if (count > 0) {
            //must容器  
            if (!mustCriterions.isEmpty()) {
                for (ESCriterion criterion : mustCriterions) {
                    for (QueryBuilder builder : criterion.listCriterion()) {
                        queryBuilder = boolQueryBuilder.must(builder);
                    }
                }
            }
            //should容器
            if (!shouldCriterions.isEmpty()) {
                for (ESCriterion criterion : shouldCriterions) {
                    for (QueryBuilder builder : criterion.listCriterion()) {
                        queryBuilder = boolQueryBuilder.should(builder);
                    }
                }
            }
            //must not 容器
            if (!mustNotCriterions.isEmpty()) {
                for (ESCriterion criterion : mustNotCriterions) {
                    for (QueryBuilder builder : criterion.listCriterion()) {
                        queryBuilder = boolQueryBuilder.mustNot(builder);
                    }
                }
            }
            return queryBuilder;
        } else {
            return null;
        }
    }

    private void must() {
        if (criterion != null) {
            mustCriterions.add(criterion);
        }
    }

    public void setQueryBuilder(Map<String, Object> map) {
        criterion = new ESCriterion();
        if (map == null || map.isEmpty()) {
            return;
        }
        for (Entry<String, Object> entry : map.entrySet()) {
            criterion.term(entry.getKey(), entry.getValue());
        }
        must();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> ESQueryBuilder setQueryBuilder(T t) {
        criterion = new ESCriterion();
        List<Field> fieldList = new ArrayList<>();
        Class tempClass = t.getClass();
        while (tempClass != null) {
            fieldList.addAll(Arrays.asList(tempClass.getDeclaredFields()));
            tempClass = tempClass.getSuperclass();
        }
        for (Field field : fieldList) {
            ESQuery esQueryAnno = field.getAnnotation(ESQuery.class);
            if (esQueryAnno == null) {
                continue;
            }
            Object fValue = null;
            try {
                field.setAccessible(true);
                fValue = field.get(t);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            if (fValue == null || fValue.equals("-1")) {
                continue;
            }
            ESQueryType type = esQueryAnno.type();
            String key = field.getName();
            if (esQueryAnno.key() != null && !esQueryAnno.key().equals("")) {
                key = esQueryAnno.key();
            }
            if (type == null) {
                //默认使用ES的term查询
                criterion.term(key, fValue);
            } else {
                switch (type) {
                    case TERMS:
						/*String split = esQueryAnno.split();
						String[] splitValue = null;
						if (split != null) {
							splitValue = fValue.toString().split(split);
						}
						if (splitValue != null) {
							criterion.terms(key, Arrays.asList(splitValue));
						} else {
							criterion.terms(key, Arrays.asList(fValue));
						}
						break;*/
                        Collection<Object> cv = (Collection<Object>) fValue;
                        criterion.terms(key, cv);
                        break;
                    case RANGE:
                        Object[] v = (Object[]) fValue;
                        if (v.length == 2) {
                            criterion.range(key, v[0], v[1]);
                        } else {
                            criterion.range(key, fValue, null);
                        }
                        break;
                    case MATCH:
                        criterion.match(key, fValue);
                        break;
                    default:
                        //默认使用ES的term查询
                        criterion.term(key, fValue);
                        break;
                }
            }
        }
        must();
        return this;
    }

    public int getSize() {
        return size;
    }

    public ESQueryBuilder setSize(int size) {
        this.size = size;
        return this;
    }

    public String getAsc() {
        return asc;
    }

    public void setAsc(String asc) {
        this.asc = asc;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public int getFrom() {
        return from;
    }

    public ESQueryBuilder setFrom(int from) {
        this.from = from;
        return this;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    class ESCriterion {
        private List<QueryBuilder> list = new ArrayList<>();

        /**
         * match 分词查询
         *
         * @param field 字段名
         * @param value 字段值
         */
        public ESCriterion match(String field, Object value) {
            list.add(QueryBuilders.matchQuery(field, value));
            return this;
        }

        /**
         * term 查询
         *
         * @param field 字段名
         * @param value 字段值
         */
        public ESCriterion term(String field, Object value) {
            list.add(QueryBuilders.termQuery(field, value));
            return this;
        }

        /**
         * terms 查询
         *
         * @param field  字段名
         * @param values 集合值
         */
        public ESCriterion terms(String field, Collection<Object> values) {
            list.add(QueryBuilders.termsQuery(field, values));
            return this;
        }

        /**
         * fuzzy 查询
         *
         * @param field 字段名
         * @param value 值
         */
        public ESCriterion fuzzy(String field, Object value) {
            list.add(QueryBuilders.fuzzyQuery(field, value));
            return this;
        }

        /**
         * range 查询
         *
         * @param from 起始值
         * @param to   末尾值
         */
        public ESCriterion range(String field, Object from, Object to) {
            list.add(QueryBuilders.rangeQuery(field).from(from).to(to).includeLower(true).includeUpper(false));
            return this;
        }

        /**
         * queryString 查询
         *
         * @param queryString 查询语句
         */
        public ESCriterion queryString(String queryString) {
            list.add(QueryBuilders.queryStringQuery(queryString));
            return this;
        }

        public List<QueryBuilder> listCriterion() {
            return list;
        }

        @Override
        public String toString() {
            return "ESCriterion [list=" + list + "]";
        }
    }

    public static class DateHistogram {
        private String fieldName;

        private DateHistogramInterval dateInterval;

        private String format;

        private Long min;

        private Long max;

        private Long minDocCount;

        private String timeZone;   // "Asia/Shanghai"

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public DateHistogramInterval getDateInterval() {
            return dateInterval;
        }

        public void setDateInterval(DateHistogramInterval dateInterval) {
            this.dateInterval = dateInterval;
        }

        public String getFormat() {
            return format;
        }

        public void setFormat(String format) {
            this.format = format;
        }

        public Long getMin() {
            return min;
        }

        public void setMin(Long min) {
            this.min = min;
        }

        public Long getMax() {
            return max;
        }

        public void setMax(Long max) {
            this.max = max;
        }

        public Long getMinDocCount() {
            return minDocCount;
        }

        public void setMinDocCount(Long minDocCount) {
            this.minDocCount = minDocCount;
        }

        public String getTimeZone() {
            return timeZone;
        }

        public void setTimeZone(String timeZone) {
            this.timeZone = timeZone;
        }
    }
}
