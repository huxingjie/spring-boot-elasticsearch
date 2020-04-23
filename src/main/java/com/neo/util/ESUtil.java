package com.neo.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Elasticsearch工具类
 */
@Component
public class ESUtil {
    private static final Logger logger = LoggerFactory.getLogger(ESUtil.class);

    private final static int MAX = 10000;

    TransportClient client;

    @Value("${whale.history.es.cluster.name:elasticsearch}")
    private String clusterName = "elasticsearch";

    @Value("${whale.history.es.cluster.address:localhost:9300}")
    private String clusterAddress;

    /**
     * client初始化
     */
    @PostConstruct
    public void init() {
        try {
            Settings settings = Settings.builder().put("cluster.name", clusterName).build();
            /**
             * 如果安装了x-pack组件，需要引入 x-pack-transport 使用PreBuiltXPackTransportClient实现 增加认证参数
             * https://www.elastic.co/guide/en/x-pack/current/java-clients.html
             */
            this.client = new PreBuiltTransportClient(settings);
            String[] nodes = clusterAddress.split(",");
            for (String node : nodes) {
                if (node.length() > 0) {
                    String[] hostPort = node.split(":");
                    this.client.addTransportAddress(
                            new TransportAddress(InetAddress.getByName(hostPort[0]), Integer.parseInt(hostPort[1])));
                }
            }
        } catch (Exception e) {
            logger.error("ES init failed!", e);
        }
    }

    /**
     * 保存记录
     *
     * @param index
     * @param type
     * @param id
     * @param object
     */
    public void insert(String index, String type, String id, Object object) {
        try {
            JSONObject source = (JSONObject) JSONObject.toJSON(object);
            IndexRequest indexRequest = new IndexRequest(index, type, id)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(source);
            client.index(indexRequest);
        } catch (Exception e) {
            logger.error("insert data failed", e);
        }
    }

    /**
     * 插入数据
     *
     * @param index  索引名
     * @param type   类型
     * @param _id    数据id
     * @param source 数据
     */
    public void insertData(String index, String type, String _id, Map<String, ?> source) throws Exception {
        client.prepareIndex(index, type).setId(_id).setSource(source).get();
    }

    /**
     * 更新数据
     *
     * @param index 索引名
     * @param type  类型
     * @param _id   数据id
     * @param field 数据字段名
     * @param value 数据字段值(布尔型)
     */
    public void updateData(String index, String type, String _id, String field, boolean value) throws Exception {
        UpdateRequest updateRequest = new UpdateRequest(index, type, _id)
                .doc(jsonBuilder().startObject().field(field, value).endObject());
        UpdateResponse resp = client.update(updateRequest).get();
        resp.getGetResult();
    }

    /**
     * 更新数据
     *
     * @param index  索引名
     * @param type   类型
     * @param _id    数据id
     * @param fields 数据字段名
     * @param values 数据字段值(布尔型)
     */
    public void updateData(String index, String type, String _id, String[] fields, Object[] values) throws Exception {
        UpdateRequest updateRequest = new UpdateRequest(index, type, _id);
        XContentBuilder builder = jsonBuilder().startObject();
        for (int i = 0; i < fields.length; i++) {
            builder.field(fields[i], values[i]);
        }
        builder.endObject();
        updateRequest.doc(builder);
        UpdateResponse resp = client.update(updateRequest).get();
        resp.getGetResult();
    }

    /**
     * 更新数据
     */
    public void updateData(String index, String type, String _id, String source) throws Exception {
        UpdateRequest updateRequest = new UpdateRequest(index, type, _id)
                .doc(source, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        UpdateResponse resp = client.update(updateRequest).get();
        resp.getGetResult();
    }

    /**
     * 更新记录
     *
     * @param index
     * @param type
     * @param id
     * @param object
     */
    public void update(String index, String type, String id, Object object) throws Exception {
        String source = JSON.toJSONString(object);
        updateData(index, type, id, source);
    }

    /**
     * 删除数据
     *
     * @param index 索引名
     * @param type  类型
     * @param _id   数据id
     */
    public void deleteData(String index, String type, String _id) throws Exception {
        client.prepareDelete(index, type, _id).get();
    }

    /**
     * 删除数据
     *
     * @param index   索引名
     * @param type    类型
     * @param builder 过滤条件
     */
    public long deleteData(String index, String type, ESQueryBuilder builder) throws Exception {
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(builder.listBuilders()).source(index)
                .get();
        return response.getDeleted();
    }

    /**
     * 删除数据
     *
     * @param index        索引名
     * @param type         类型
     * @param queryBuilder 过滤条件
     */
    public long deleteData(String index, String type, QueryBuilder queryBuilder) throws Exception {
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(queryBuilder).source(index)
                .get();
        return response.getDeleted();
    }

    /**
     * 批量插入数据
     *
     * @param index 索引名
     * @param type  类型
     * @param data  (_id 主键, json 数据)
     */
    public void bulkInsertData(String index, String type, Map<String, Object> data) throws Exception {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        data.forEach((param1, param2) -> {
            bulkRequest.add(client.prepareIndex(index, type, param1)
                    .setSource(param2)
            );
        });
        bulkRequest.get();
    }

    /**
     * 批量插入数据
     *
     * @param index 索引名
     * @param type  类型
     * @param data  批量数据
     */
    public void bulkInsertData(String index, String type, List<Map<String, Object>> data) throws Exception {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        data.forEach(item -> {
            bulkRequest.add(client.prepareIndex(index, type).setSource(item)
            );
        });
        bulkRequest.get();
    }

    /**
     * 查询
     *
     * @param index 索引名
     * @param type  类型
     * @param _id   主键
     */
    public String searchById(String index, String type, String _id) throws Exception {
        return client.prepareGet(index, type, _id).get().getSourceAsString();
    }

    /**
     * 查询
     *
     * @param index 索引名
     * @param type  类型
     * @param _id   主键
     */
    public Map<String, Object> searchMapById(String index, String type, String _id) throws Exception {
        return client.prepareGet(index, type, _id).get().getSourceAsMap();
    }

    /**
     * 查询
     *
     * @param index   索引名
     * @param type    类型
     * @param builder 查询构造
     */
    public List<String> search(String index, String type, ESQueryBuilder builder) throws Exception {
        List<String> result = new ArrayList<>();
        SearchRequestBuilder searchRequestBuilder = createSearchRequestBuilder(index, type, builder)
                .addSort(createSortBuilder(builder));
        SearchResponse sr = searchRequestBuilder.execute().actionGet();
        SearchHits hits = sr.getHits();
        SearchHit[] searchHists = hits.getHits();
        for (SearchHit sh : searchHists) {
            result.add(sh.getSourceAsString());
        }
        return result;
    }

    /**
     * 分页查询
     *
     * @param index 索引名
     * @param type  类型
     */
    public SearchResponse searchResponse(String index, String type, QueryBuilder queryBuilder, FieldSortBuilder sortBuilder, Integer from, Integer size)
            throws Exception {
        return searchResponse(index, type, queryBuilder, sortBuilder, null, from, size);
    }

    /**
     * 分页查询
     *
     * @param index 索引名
     * @param type  类型
     */
    public SearchResponse searchResponse(String index, String type, QueryBuilder queryBuilder, FieldSortBuilder sortBuilder, String[] includes, Integer from, Integer size)
            throws Exception {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setQuery(queryBuilder);
        if (includes != null && includes.length > 0) {
            searchRequestBuilder.setFetchSource(includes, Strings.EMPTY_ARRAY);
        }
        if (sortBuilder != null) {
            searchRequestBuilder.addSort(sortBuilder);
        }
        if (size != null) {
            size = size < 0 ? 0 : size;
            size = size > MAX ? MAX : size;
            //返回条目数
            searchRequestBuilder.setSize(size);
        }
        if (from != null) {
            searchRequestBuilder.setFrom(from);
        }
        SearchResponse sr = searchRequestBuilder.execute().actionGet();
        return sr;
    }

    /**
     * 滑动分页查询(prepare)
     */
    public SearchResponse scrollSearchPrepare(String index, String type, QueryBuilder queryBuilder, FieldSortBuilder sortBuilder, Integer size, Long keepAlive)
            throws Exception {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setQuery(queryBuilder);
        if (sortBuilder != null) {
            searchRequestBuilder.addSort(sortBuilder);
        }
        if (size != null) {
            size = size < 0 ? 0 : size;
            //返回条目数
            searchRequestBuilder.setSize(size);
        }
        searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH).setScroll(new TimeValue(keepAlive));
        SearchResponse sr = searchRequestBuilder.execute().actionGet();
        return sr;
    }

    /**
     * 滑动分页查询(do)
     */
    public SearchResponse scrollSearch(String scrollId, Long keepAlive) {
        SearchResponse searchResponse = client.prepareSearchScroll(scrollId)
                .setScroll(new TimeValue(keepAlive))
                .execute().actionGet();
        return searchResponse;
    }

    /**
     * 聚合查询
     *
     * @param index 索引名
     * @param type  类型
     */
    public LinkedHashMap<String, Long> searchAggResponse(String index, String type, QueryBuilder queryBuilder, FieldSortBuilder sortBuilder, String groupBy,
                                                         String script, Integer from, Integer size, Integer shardSize) throws Exception {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setQuery(queryBuilder);
        if (sortBuilder != null) {
            searchRequestBuilder.addSort(sortBuilder);
        }
        if (StringUtils.isEmpty(groupBy)) {
            Script groupScript = new Script(ScriptType.INLINE, "painless", script, new HashMap<String, Object>());
            searchRequestBuilder.addAggregation(
                    AggregationBuilders.terms("agg").script(groupScript).order(BucketOrder.count(false)).size(size.intValue()).shardSize(shardSize)
            );
        } else {
            searchRequestBuilder.addAggregation(
                    AggregationBuilders.terms("agg").field(groupBy).order(BucketOrder.count(false)).size(size.intValue()).shardSize(shardSize)
            );
        }
        searchRequestBuilder.setSize(0);
        if (from != null) {
            searchRequestBuilder.setFrom(from);
        }
        SearchResponse sr = searchRequestBuilder.execute().actionGet();
        LinkedHashMap<String, Long> map = new LinkedHashMap<>();
        if (sr == null || sr.getHits().getTotalHits() == 0) {
            logger.error("searchAggResponse return empty");
            return map;
        }
        Terms agg = sr.getAggregations().get("agg");
        Iterator<Bucket> iter = (Iterator<Bucket>) agg.getBuckets().iterator();
        while (iter.hasNext()) {
            Bucket gradeBucket = iter.next();
            map.put(gradeBucket.getKey().toString(), gradeBucket.getDocCount());
        }
        return map;
    }
//
//    /**
//     * 聚合查询返回人像统计信息
//     */
//    public List<AlarmTargetItem> searchAggTargets(QueryBuilder queryBuilder, FieldSortBuilder sortBuilder, Integer size) throws Exception {
//        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(WhaleTaskConstant.ALARM_INDEX).setTypes(WhaleTaskConstant.ALARM_TYPE).setQuery(queryBuilder);
//        if (sortBuilder != null) {
//            searchRequestBuilder.addSort(sortBuilder);
//        }
//        String[] includes = {"tarIdentityId", "tarName", "tarLibSerial", "tarLibName", "tarCoverImageSerial", "tarCoverImageUrl"};
//        AggregationBuilder aggregation = AggregationBuilders.terms("agg").field("targetSerial.keyword")
//                .subAggregation(AggregationBuilders.topHits("top").fetchSource(includes, Strings.EMPTY_ARRAY).size(1)).size(size).shardSize(size);
//        searchRequestBuilder.addAggregation(aggregation);
//        searchRequestBuilder.setSize(0);
//        SearchResponse sr = searchRequestBuilder.execute().actionGet();
//        List<AlarmTargetItem> list = Lists.newArrayList();
//        if (sr == null || sr.getHits().getTotalHits() == 0) {
//            logger.error("searchAggResponse return empty");
//            return list;
//        }
//        Terms agg = sr.getAggregations().get("agg");
//        Iterator<Bucket> iter = (Iterator<Bucket>) agg.getBuckets().iterator();
//        while (iter.hasNext()) {
//            Bucket bucket = iter.next();
//            AlarmTargetItem targetItem = new AlarmTargetItem();
//            targetItem.setTargetSerial(bucket.getKey().toString());
//            targetItem.setCount(bucket.getDocCount());
//            if (bucket.getAggregations() != null) {
//                TopHits topHits = bucket.getAggregations().get("top");
//                for (SearchHit hit : topHits.getHits()) {
//                    Map<String, Object> map = hit.getSourceAsMap();
//                    targetItem.setIdentityId((String) map.get("tarIdentityId"));
//                    targetItem.setTargetName((String) map.get("tarName"));
//                    targetItem.setTarLibName((String) map.get("tarLibName"));
//                    targetItem.setTarCoverImageSerial((String) map.get("tarCoverImageSerial"));
//                    String tarLibSerial = (String) map.get("tarLibSerial");
//                    targetItem.setTarLibSerial(tarLibSerial);
//                    targetItem.setTarCoverImageUrl(syncManager.getImageUrl(tarLibSerial, (String) map.get("tarCoverImageUrl")));
//                }
//            }
//            list.add(targetItem);
//        }
//        return list;
//    }
//
//    /**
//     * 按人聚合查询返回人像及top条目信息
//     * 人像告警明细按抓拍时间倒序排序
//     *
//     * @param queryBuilder       过滤条件
//     * @param sortType           0按最新一条抓拍时间倒序，1按告警数量倒序排序
//     * @param aggTargetSize      人像数量
//     * @param aggTargetTrackSize 每个人像的告警数量
//     * @return
//     * @throws Exception
//     */
//    public AggTargetListResponse aggTargetList(QueryBuilder queryBuilder, int sortType, int aggTargetSize, int aggTargetTrackSize) throws Exception {
//        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(WhaleTaskConstant.ALARM_INDEX).setTypes(WhaleTaskConstant.ALARM_TYPE).setQuery(queryBuilder);
//        TermsAggregationBuilder aggregation = AggregationBuilders.terms("agg").field("targetSerial.keyword");
//        if (sortType == 0) { // 按最新的一条告警抓拍时间倒序
//            aggregation.order(BucketOrder.aggregation("max_time", false))
//                    .subAggregation(AggregationBuilders.max("max_time").field("captureTime"));
//        } else { // 按聚合数量倒序
//            aggregation.order(BucketOrder.count(false));
//        }
//        // 设置目标人的告警明细记录
//        aggregation.subAggregation(AggregationBuilders.topHits("top").sort("captureTime", SortOrder.DESC).size(aggTargetTrackSize))
//                .size(aggTargetSize).shardSize(aggTargetSize);
//        searchRequestBuilder.addAggregation(aggregation);
//        searchRequestBuilder.setSize(0);
//        SearchResponse sr = searchRequestBuilder.execute().actionGet();
//        AggTargetListResponse response = new AggTargetListResponse();
//        List<AggTargetListItem> list = Lists.newArrayList();
//        response.setList(list);
//        if (sr == null || sr.getHits().getTotalHits() == 0) {
//            logger.error("aggTargetList return empty");
//            return response;
//        }
//        Terms agg = sr.getAggregations().get("agg");
//        Iterator<Bucket> iter = (Iterator<Bucket>) agg.getBuckets().iterator();
//        while (iter.hasNext()) {
//            Bucket bucket = iter.next();
//            AggTargetListItem aggListItem = new AggTargetListItem();
//            list.add(aggListItem);
//            AlarmTarget target = new AlarmTarget();
//            List<AlarmTargetTrack> tracks = new ArrayList<>();
//            aggListItem.setTarget(target);
//            aggListItem.setTracks(tracks);
//            aggListItem.setCount(bucket.getDocCount());
//            target.setTargetSerial(bucket.getKey().toString());
//            if (bucket.getAggregations() != null) {
//                TopHits topHits = bucket.getAggregations().get("top");
//                boolean isFirst = true;
//                for (SearchHit hit : topHits.getHits()) {
//                    Map<String, Object> map = hit.getSourceAsMap();
//                    String tarLibSerial = (String) map.get("tarLibSerial");
//                    if (isFirst) {
//                        target.setTarLibName((String) map.get("tarLibName"));
//                        target.setTarLibSerial(tarLibSerial);
//                        target.setTarIdentityId((String) map.get("tarIdentityId"));
//                        target.setTarName((String) map.get("tarName"));
//                        target.setTarNatives((String) map.get("tarNatives"));
//                        target.setTarBirthday(map.get("tarBirthday") == null ? null : Long.valueOf(map.get("tarBirthday").toString()));
//                        target.setTarGender((Integer) map.get("tarGender"));
//                        target.setSnative((String) map.get("snative"));
//                        target.setIsPrivate((Integer) map.get("isPrivate"));
//                        target.setTarCoverImageSerial((String) map.get("tarCoverImageSerial"));
//                        target.setTarCoverImageUrl(syncManager.getImageUrl(tarLibSerial, (String) map.get("tarCoverImageUrl")));
//                        target.setTarRemark((String) map.get("tarRemark"));
//                    }
//                    AlarmTargetTrack targetTrack = new AlarmTargetTrack();
//                    targetTrack.setEsId((String) map.get("esId"));
//                    targetTrack.setCaptureTime(map.get("captureTime") == null ? null : Long.valueOf(map.get("captureTime").toString()));
//                    targetTrack.setReceivedTime(map.get("receivedTime") == null ? null : Long.valueOf(map.get("receivedTime").toString()));
//                    targetTrack.setTarUrl(syncManager.getImageUrl(tarLibSerial, (String) map.get("tarUrl")));
//                    targetTrack.setTarImageSerial((String) map.get("tarImageSerial"));
//                    targetTrack.setScore(map.get("score") == null ? 0 : Float.valueOf(map.get("score").toString()));
//                    targetTrack.setCameraAddress((String) map.get("cameraAddress"));
//                    targetTrack.setCameraSerial((String) map.get("cameraSerial"));
//                    targetTrack.setCameraName((String) map.get("cameraName"));
//                    targetTrack.setTaskSerial((String) map.get("taskSerial"));
//                    targetTrack.setTaskName((String) map.get("taskName"));
//                    targetTrack.setMatched((Boolean) map.get("matched"));
//                    String bigImage = (String) map.get("bigImage");
//                    if (bigImage != null
//                            && !bigImage.startsWith("http://")
//                            && !bigImage.startsWith("https://")) {
//                        targetTrack.setBigImage(fastDfsPrefix + bigImage);
//                    } else {
//                        targetTrack.setBigImage(bigImage);
//                    }
//                    String faceUrl = (String) map.get("faceUrl");
//                    if (faceUrl != null
//                            && !faceUrl.startsWith("http://")
//                            && !faceUrl.startsWith("https://")) {
//                        targetTrack.setFaceUrl(fastDfsPrefix + faceUrl);
//                    } else {
//                        targetTrack.setFaceUrl(faceUrl);
//                    }
//                    targetTrack.setLongitude((String) map.get("longitude"));
//                    targetTrack.setLatitude((String) map.get("latitude"));
//                    targetTrack.setCameraGroup((String) map.get("cameraGroup"));
//                    targetTrack.setCameraGroupPath((String) map.get("cameraGroupPath"));
//                    tracks.add(targetTrack);
//                }
//            }
//        }
//        // 查询聚合桶的总数量
//        long realTotal = queryBucketCount(WhaleTaskConstant.ALARM_INDEX, WhaleTaskConstant.ALARM_TYPE, queryBuilder, "targetSerial.keyword");
//        response.setRealTotal(realTotal);
//        response.setTotal(Math.min(aggTargetSize, realTotal));
//        return response;
//    }
//
//    /**
//     * 按视频源聚合查询返回人像及top条目信息
//     * 人像告警明细按抓拍时间倒序排序
//     *
//     * @param queryBuilder         过滤条件
//     * @param sortType             0按最新一条抓拍时间倒序，1按告警数量倒序排序
//     * @param aggResourceSize      人像数量
//     * @param aggResourceTrackSize 每个人像的告警数量
//     * @return
//     * @throws Exception
//     */
//    public AggResourceListResponse aggResourceList(QueryBuilder queryBuilder, int sortType, int aggResourceSize, int aggResourceTrackSize) throws Exception {
//        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(WhaleTaskConstant.ALARM_INDEX).setTypes(WhaleTaskConstant.ALARM_TYPE).setQuery(queryBuilder);
//        TermsAggregationBuilder aggregation = AggregationBuilders.terms("agg").field("cameraSerial.keyword");
//        if (sortType == 0) { // 按最新的一条告警抓拍时间倒序
//            aggregation.order(BucketOrder.aggregation("max_time", false))
//                    .subAggregation(AggregationBuilders.max("max_time").field("captureTime"));
//        } else { // 按聚合数量倒序
//            aggregation.order(BucketOrder.count(false));
//        }
//        // 设置目标人的告警明细记录
//        aggregation.subAggregation(AggregationBuilders.topHits("top").sort("captureTime", SortOrder.DESC).size(aggResourceTrackSize))
//                .size(aggResourceSize).shardSize(aggResourceSize);
//        searchRequestBuilder.addAggregation(aggregation);
//        searchRequestBuilder.setSize(0);
//        SearchResponse sr = searchRequestBuilder.execute().actionGet();
//        AggResourceListResponse response = new AggResourceListResponse();
//        List<AggResourceListItem> list = Lists.newArrayList();
//        response.setList(list);
//        if (sr == null || sr.getHits().getTotalHits() == 0) {
//            logger.error("aggResourceList return empty");
//            return response;
//        }
//        Terms agg = sr.getAggregations().get("agg");
//        Iterator<Bucket> iter = (Iterator<Bucket>) agg.getBuckets().iterator();
//        while (iter.hasNext()) {
//            Bucket bucket = iter.next();
//            AggResourceListItem aggListItem = new AggResourceListItem();
//            list.add(aggListItem);
//            AlarmResource resource = new AlarmResource();
//            List<AlarmResourceTrack> tracks = new ArrayList<>();
//            aggListItem.setResource(resource);
//            aggListItem.setTracks(tracks);
//            aggListItem.setCount(bucket.getDocCount());
//            resource.setCameraSerial(bucket.getKey().toString());
//            if (bucket.getAggregations() != null) {
//                TopHits topHits = bucket.getAggregations().get("top");
//                boolean isFirst = true;
//                for (SearchHit hit : topHits.getHits()) {
//                    Map<String, Object> map = hit.getSourceAsMap();
//                    if (isFirst) {
//                        resource.setCameraName((String) map.get("cameraName"));
//                        resource.setCameraGroup((String) map.get("cameraGroup"));
//                        resource.setCameraGroupPath((String) map.get("cameraGroupPath"));
//                        resource.setCameraAddress((String) map.get("cameraAddress"));
//                        resource.setLatitude((String) map.get("latitude"));
//                        resource.setLongitude((String) map.get("longitude"));
//                    }
//                    AlarmResourceTrack resourceTrack = new AlarmResourceTrack();
//                    String tarLibSerial = (String) map.get("tarLibSerial");
//                    resourceTrack.setTargetSerial((String) map.get("targetSerial"));
//                    resourceTrack.setEsId((String) map.get("esId"));
//                    resourceTrack.setCaptureTime(map.get("captureTime") == null ? null : Long.valueOf(map.get("captureTime").toString()));
//                    resourceTrack.setReceivedTime(map.get("receivedTime") == null ? null : Long.valueOf(map.get("receivedTime").toString()));
//                    resourceTrack.setScore(map.get("score") == null ? 0 : Float.valueOf(map.get("score").toString()));
//                    resourceTrack.setTarLibName((String) map.get("tarLibName"));
//                    resourceTrack.setTarLibSerial((String) map.get("tarLibSerial"));
//                    resourceTrack.setTarIdentityId((String) map.get("tarIdentityId"));
//                    resourceTrack.setTarName((String) map.get("tarName"));
//                    resourceTrack.setTarUrl(syncManager.getImageUrl(tarLibSerial, (String) map.get("tarUrl")));
//                    resourceTrack.setTarNatives((String) map.get("tarNatives"));
//                    resourceTrack.setTarBirthday(map.get("tarBirthday") == null ? null : Long.valueOf(map.get("tarBirthday").toString()));
//                    resourceTrack.setTarGender((Integer) map.get("tarGender"));
//                    resourceTrack.setTarRemark((String) map.get("tarRemark"));
//                    resourceTrack.setTaskSerial((String) map.get("taskSerial"));
//                    resourceTrack.setTaskName((String) map.get("taskName"));
//                    resourceTrack.setMatched((Boolean) map.get("matched"));
//                    String bigImage = (String) map.get("bigImage");
//                    if (bigImage != null
//                            && !bigImage.startsWith("http://")
//                            && !bigImage.startsWith("https://")) {
//                        resourceTrack.setBigImage(fastDfsPrefix + bigImage);
//                    } else {
//                        resourceTrack.setBigImage(bigImage);
//                    }
//                    String faceUrl = (String) map.get("faceUrl");
//                    if (faceUrl != null
//                            && !faceUrl.startsWith("http://")
//                            && !faceUrl.startsWith("https://")) {
//                        resourceTrack.setFaceUrl(fastDfsPrefix + faceUrl);
//                    } else {
//                        resourceTrack.setFaceUrl(faceUrl);
//                    }
//                    resourceTrack.setSnative((String) map.get("snative"));
//                    resourceTrack.setIsPrivate((Integer) map.get("isPrivate"));
//                    resourceTrack.setTarImageSerial((String) map.get("tarImageSerial"));
//                    resourceTrack.setTarCoverImageSerial((String) map.get("tarCoverImageSerial"));
//                    resourceTrack.setTarCoverImageUrl(syncManager.getImageUrl(tarLibSerial, (String) map.get("tarCoverImageUrl")));
//                    tracks.add(resourceTrack);
//                }
//            }
//        }
//        // 查询聚合桶的总数量
//        long realTotal = queryBucketCount(WhaleTaskConstant.ALARM_INDEX, WhaleTaskConstant.ALARM_TYPE, queryBuilder, "cameraSerial.keyword");
//        response.setRealTotal(realTotal);
//        response.setTotal(Math.min(aggResourceSize, response.getRealTotal()));
//        return response;
//    }
//
//    /**
//     * 查询聚合桶的总数量
//     */
//    private long queryBucketCount(String index, String tpye, QueryBuilder queryBuilder, String aggKey) {
//        SearchRequestBuilder requestBuilder = client.prepareSearch(WhaleTaskConstant.ALARM_INDEX).setTypes(WhaleTaskConstant.ALARM_TYPE).setQuery(queryBuilder);
//        requestBuilder.addAggregation(AggregationBuilders.cardinality("cardAgg").field(aggKey));
//        requestBuilder.setSize(0);
//        SearchResponse searchResponse = requestBuilder.execute().actionGet();
//        Cardinality tarAgg = searchResponse.getAggregations().get("cardAgg");
//        return tarAgg.getValue();
//    }
//
//    /**
//     * 按人统计告警次数最多的视频源（常现点，常现次数）
//     */
//    public ResourceTopByTargetResponse resourceTopByTarget(QueryBuilder queryBuilder, int aggResourceSize) throws Exception {
//        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(WhaleTaskConstant.ALARM_INDEX).setTypes(WhaleTaskConstant.ALARM_TYPE).setQuery(queryBuilder);
//        TermsAggregationBuilder aggregation = AggregationBuilders.terms("agg1").field("targetSerial.keyword");
//        TermsAggregationBuilder subAggregation = AggregationBuilders.terms("agg2").field("cameraSerial.keyword").order(BucketOrder.count(false)).size(1);
//        subAggregation.subAggregation(AggregationBuilders.topHits("agg2-top").fetchSource(new String[]{"cameraName", "cameraSerial"}, Strings.EMPTY_ARRAY)
//                .sort("captureTime", SortOrder.DESC).size(1));
//        // 设置目标人的告警明细记录
//        aggregation.subAggregation(subAggregation).size(aggResourceSize).shardSize(aggResourceSize);
//        searchRequestBuilder.addAggregation(aggregation);
//        searchRequestBuilder.setSize(0);
//        SearchResponse sr = searchRequestBuilder.execute().actionGet();
//        ResourceTopByTargetResponse response = new ResourceTopByTargetResponse();
//        List<ResourceTopByTargetItem> list = Lists.newArrayList();
//        response.setList(list);
//        if (sr == null || sr.getHits().getTotalHits() == 0) {
//            logger.error("resourceTopByTarget return empty");
//            return response;
//        }
//        Terms agg = sr.getAggregations().get("agg1");
//        Iterator<Bucket> iter = (Iterator<Bucket>) agg.getBuckets().iterator();
//        response.setTotal(agg.getBuckets().size());
//        while (iter.hasNext()) {
//            Bucket bucket = iter.next();
//            ResourceTopByTargetItem item = new ResourceTopByTargetItem();
//            item.setTargetSerial(bucket.getKey().toString());
//            list.add(item);
//            if (bucket.getAggregations() != null) {
//                Terms agg2 = bucket.getAggregations().get("agg2");
//                Iterator<Bucket> iter2 = (Iterator<Bucket>) agg2.getBuckets().iterator();
//                if (iter2.hasNext()) {
//                    Bucket bucket2 = iter2.next();
//                    item.setCameraSerial(bucket2.getKey().toString());
//                    item.setCount(bucket2.getDocCount());
//                    TopHits topHits = bucket2.getAggregations().get("agg2-top");
//                    for (SearchHit hit : topHits.getHits()) {
//                        Map<String, Object> map = hit.getSourceAsMap();
//                        item.setCameraName((String) map.get("cameraName"));
//                    }
//                }
//            }
//        }
//        return response;
//    }
//
//    /**
//     * 聚合查询告警数TopN个目标人
//     * 按告警数排序
//     */
//    public List<AlarmTargetTopItem> searchAggTargetsTop(QueryBuilder queryBuilder, Integer size) throws Exception {
//        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(WhaleTaskConstant.ALARM_INDEX).setTypes(WhaleTaskConstant.ALARM_TYPE).setQuery(queryBuilder);
//        String[] includes = {"tarIdentityId", "tarName", "tarLibSerial", "tarLibName", "tarCoverImageSerial", "tarCoverImageUrl"};
//        AggregationBuilder aggregation = AggregationBuilders.terms("agg").field("targetSerial.keyword").order(BucketOrder.count(false))
//                .subAggregation(AggregationBuilders.topHits("top").fetchSource(includes, Strings.EMPTY_ARRAY).size(1)).size(size).shardSize(size);
//        searchRequestBuilder.addAggregation(aggregation);
//        searchRequestBuilder.setSize(0);
//        SearchResponse sr = searchRequestBuilder.execute().actionGet();
//        List<AlarmTargetTopItem> list = Lists.newArrayList();
//        if (sr == null || sr.getHits().getTotalHits() == 0) {
//            logger.error("searchAggTargetsTop return empty");
//            return list;
//        }
//        Terms agg = sr.getAggregations().get("agg");
//        Iterator<Bucket> iter = (Iterator<Bucket>) agg.getBuckets().iterator();
//        while (iter.hasNext()) {
//            Bucket bucket = iter.next();
//            AlarmTargetTopItem targetItem = new AlarmTargetTopItem();
//            targetItem.setTargetSerial(bucket.getKey().toString());
//            targetItem.setCount(bucket.getDocCount());
//            if (bucket.getAggregations() != null) {
//                TopHits topHits = bucket.getAggregations().get("top");
//                for (SearchHit hit : topHits.getHits()) {
//                    Map<String, Object> map = hit.getSourceAsMap();
//                    targetItem.setIdentityId((String) map.get("tarIdentityId"));
//                    targetItem.setTargetName((String) map.get("tarName"));
//                    targetItem.setTarLibName((String) map.get("tarLibName"));
//                    targetItem.setTarCoverImageSerial((String) map.get("tarCoverImageSerial"));
//                    String tarLibSerial = (String) map.get("tarLibSerial");
//                    targetItem.setTarLibSerial(tarLibSerial);
//                    targetItem.setTarCoverImageUrl(syncManager.getImageUrl(tarLibSerial, (String) map.get("tarCoverImageUrl")));
//                }
//            }
//            list.add(targetItem);
//        }
//        return list;
//    }

    /**
     * 时间分组统计
     *
     * @param index
     * @param type
     * @param field
     * @param builder
     */
    public Map<String, Long> aggAlarmCountByDay(String index, String type, DateHistogramAggregationBuilder field, QueryBuilder builder)
            throws Exception {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setQuery(builder);
        Map<String, Long> map = Maps.newLinkedHashMap();
        searchRequestBuilder.addAggregation(field).setSize(0);
        SearchResponse response = searchRequestBuilder.execute().actionGet();
        Histogram histogram = response.getAggregations().get("alarmCount");
        for (Histogram.Bucket entry : histogram.getBuckets()) {
            String keyAsString = entry.getKeyAsString();
            Long count = entry.getDocCount();
            map.put(keyAsString, count);
        }
        return map;
    }

    /**
     * 查询并聚合
     *
     * @param index 索引名
     * @param type  类型
     */
    public ConcurrentHashMap<String, HashSet<String>> fetchAllRecordsByScrollId(String index, String type, QueryBuilder queryBuilder) {
        //要将所有命中的记录取出，拿到他的taskSerial，最终统计有多少有效的TaskSerial，用来做分页的依据
        ConcurrentHashMap<String, HashSet<String>> targetsByTask = new ConcurrentHashMap();
        //指定一个index和type
        SearchRequestBuilder search = client.prepareSearch(index).setTypes(type).setFetchSource(new String[]{"tarName", "tarIdentityId", "taskSerial", "targetSerial"}, null);
        //使用原生排序优化性能
        search.addSort("_doc", SortOrder.ASC);
        //设置每批读取的数据量
        search.setSize(100);
        //默认是查询所有
        search.setQuery(queryBuilder);
        //设置 search context 维护1分钟的有效期
        search.setScroll(TimeValue.timeValueMinutes(1));
        //获得首次的查询结果
        SearchResponse scrollResp = search.get();
        do {
            //读取结果集数据
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                String key = hit.getSourceAsMap().get("taskSerial").toString(); //taskSerial作为key
                if (targetsByTask.get(key) != null) {
                    HashSet<String> targets = targetsByTask.get(key);
                    targets.add(hit.getSourceAsMap().get("targetSerial").toString());
                } else {
                    HashSet<String> targets = Sets.newHashSet();
                    targets.add(hit.getSourceAsMap().get("targetSerial").toString());
                    targetsByTask.put(key, targets);
                }
            }
            //将scorllId循环传递
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(TimeValue.timeValueMinutes(1)).execute().actionGet();
            //当searchHits的数组为空的时候结束循环，至此数据全部读取完毕
        } while (scrollResp.getHits().getHits().length != 0);
        return targetsByTask;
    }

    /**
     * 查询
     *
     * @param index 索引名
     * @param type  类型
     */
    public List<String> searchStr(String index, String type, QueryBuilder builder, FieldSortBuilder sortBuilder, Integer from, Integer size)
            throws Exception {
        List<String> ret = new LinkedList<>();
        SearchResponse sr = searchResponse(index, type, builder, sortBuilder, from, size);
        SearchHits hits = sr.getHits();
        SearchHit[] searchHists = hits.getHits();
        for (SearchHit sh : searchHists) {
            ret.add(sh.getSourceAsString());
        }
        return ret;
    }

    /**
     * 计数
     *
     * @param index 索引名
     * @param type  类型
     */
    public long statCount(String index, String type, ESQueryBuilder builder) throws Exception {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type)
                .setQuery(builder.listBuilders());
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        return searchResponse.getHits().totalHits;
    }

    /**
     * 计数
     *
     * @param index 索引名
     * @param type  类型
     */
    public long statCount(String index, String type, QueryBuilder builder) throws Exception {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type)
                .setQuery(builder)
                .setSize(0);
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        return searchResponse.getHits().totalHits;
    }

    /**
     * 查询单条记录
     *
     * @param index
     * @param type
     * @param builder
     */
    public String searchOne(String index, String type, ESQueryBuilder builder) throws Exception {
        SearchRequestBuilder searchRequestBuilder = createSearchRequestBuilder(index, type, builder)
                .addSort(createSortBuilder(builder));
        searchRequestBuilder.setSize(1);
        SearchResponse searchResponse = searchRequestBuilder.get();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHists = hits.getHits();
        if (searchHists.length == 0) {
            return null;
        }
        return searchHists[0].getSourceAsString();
    }

    /**
     * 分组统计查询(只统计每个分组数量, 不包含每个分组下的数据项)
     *
     * @param index
     * @param type
     * @param builder
     * @param groupBy
     */
    @SuppressWarnings("unchecked")
    public Map<Object, Object> statSearch(String index, String type, ESQueryBuilder builder, String groupBy) throws Exception {
        Map<Object, Object> map = new HashMap<>();
        builder = new ESQueryBuilder();
        builder.setQueryBuilder(map);
        SearchRequestBuilder searchRequestBuilder = createSearchRequestBuilder(index, type, builder)
                .addSort(createSortBuilder(builder));
        searchRequestBuilder.addAggregation(
                AggregationBuilders.terms("agg").field(groupBy)
        );
        SearchResponse sr = searchRequestBuilder.get();
        Terms agg = sr.getAggregations().get("agg");
        Iterator<Bucket> iter = (Iterator<Bucket>) agg.getBuckets().iterator();
        while (iter.hasNext()) {
            Bucket gradeBucket = iter.next();
            map.put(gradeBucket.getKey(), gradeBucket.getDocCount());
        }
        return map;
    }

    /**
     * 日期分组统计
     *
     * @param index
     * @param type
     * @param builder
     * @param dh
     */
    public List<InternalDateHistogram.Bucket> aggByDateHistogram(String index, String type, QueryBuilder builder, ESQueryBuilder.DateHistogram dh)
            throws Exception {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
        searchRequestBuilder.setQuery(builder);
        DateHistogramAggregationBuilder dhab = AggregationBuilders
                .dateHistogram("agg")
                .field(dh.getFieldName())
                .order(BucketOrder.key(false))
                .dateHistogramInterval(dh.getDateInterval());
        if (StringUtils.isNotBlank(dh.getFormat())) {
            dhab.format(dh.getFormat());
        }
        if (StringUtils.isNotBlank(dh.getTimeZone())) {
            dhab.timeZone(DateTimeZone.forID(dh.getTimeZone()));
        }
        if (dh.getMinDocCount() != null) {
            dhab.minDocCount(dh.getMinDocCount());
        }
        if (dh.getMin() != null && dh.getMax() != null) {
            dhab.extendedBounds(new ExtendedBounds(dh.getMin(), dh.getMax()));
        }
        searchRequestBuilder.addAggregation(dhab);
        searchRequestBuilder.setSize(0);
        SearchResponse sr = searchRequestBuilder.get();
        List<InternalDateHistogram.Bucket> buckets = ((InternalDateHistogram) sr.getAggregations().get("agg")).getBuckets();
        return buckets;
    }

    /**
     * 构造QueryBuilder
     *
     * @param index
     * @param type
     * @param builder
     * @return
     */
    private SearchRequestBuilder createSearchRequestBuilder(String index, String type, ESQueryBuilder builder) throws Exception {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
        searchRequestBuilder.setQuery(builder.listBuilders());
        int size = builder.getSize();
        if (size < 0) {
            size = 0;
        }
        if (size > MAX) {
            size = MAX;
        }
        //返回条目数  
        builder.setSize(size);
        builder.setFrom(builder.getFrom() < 0 ? 0 : builder.getFrom());
        searchRequestBuilder.setSize(size);
        searchRequestBuilder.setFrom(builder.getFrom());
        return searchRequestBuilder;
    }

    /**
     * 构造排序Builder
     *
     * @param queryBuilder
     * @return
     */
    private FieldSortBuilder createSortBuilder(ESQueryBuilder queryBuilder) {
        FieldSortBuilder sortBuilder = null;
        if (queryBuilder.getAsc() != null && queryBuilder.getAsc().length() > 0) {
            sortBuilder = new FieldSortBuilder(queryBuilder.getAsc());
            sortBuilder.order(SortOrder.ASC);
            sortBuilder.unmappedType("date");
        }
        if (queryBuilder.getDesc() != null && queryBuilder.getDesc().length() > 0) {
            sortBuilder = new FieldSortBuilder(queryBuilder.getDesc());
            sortBuilder.order(SortOrder.DESC);
            sortBuilder.unmappedType("date");
        }
        return sortBuilder;
    }

    /**
     * 关闭链接
     */
    public void close() {
        client.close();
    }

    public TransportClient getClient() {
        return client;
    }
}