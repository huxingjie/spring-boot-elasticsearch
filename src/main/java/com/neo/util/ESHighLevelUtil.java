package com.neo.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * @author chenxiaojian
 * @ClassName: ESUtil
 * @Description: Elasticsearch工具类
 * @date 2018年1月9日 下午5:03:34
 */
@Slf4j
public class ESHighLevelUtil {

    private static final Logger logger = LoggerFactory.getLogger(ESUtil.class);

    private final static int MAX = 10000;

    private RestClientBuilder restClientBuilder;

    private RestHighLevelClient restHighLevelClient;

    private static final int ADDRESS_LENGTH = 2;

    private static final String ipAddress = "localhost:9200";

    private static final String HTTP_SCHEME = "http";

    static {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
    }
    //	private String clusterAddress = "172.18.0.153:10229";
//    @PostConstruct
//    public void init() {
////        try {
////            new ESHighLevelUtil(new String[]{"localhost:9300"});
////        } catch (Exception e) {
////            logger.error("ES init failed!", e);
////        }
//    }

    public ESHighLevelUtil(String[]  ipAddress) throws Exception {
        if (restHighLevelClient == null) {
            HttpHost[] hosts = Arrays.stream(ipAddress)
                    .map(this::makeHttpHost)
                    .filter(Objects::nonNull)
                    .toArray(HttpHost[]::new);
            log.debug("hosts:{}", Arrays.toString(hosts));
            restClientBuilder = RestClient.builder(hosts);
            restClientBuilder.setMaxRetryTimeoutMillis(MAX);
            restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        }
    }

    private HttpHost makeHttpHost(String s) {
        assert StringUtils.isNotEmpty(s);
        String[] address = s.split(":");
        if (address.length == ADDRESS_LENGTH) {
            String ip = address[0];
            int port = Integer.parseInt(address[1]);
            return new HttpHost(ip, port, HTTP_SCHEME);
        } else {
            return null;
        }
    }

    public void insertData(String index, String type, String id, JSONObject source) {
        IndexRequest indexRequest = new IndexRequest(index, type, id).source(source);
        try {
            restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("insert data failed", e);
        }
    }

    public String searchById(String index, String type, String id) {
        try {
            GetRequest getRequest = new GetRequest(index, type, id);
            GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
            return getResponse.getSourceAsString();
        } catch (IOException e) {
            log.error("get data failed", e);
        }
        return null;
    }

    public String updateData(String index, String type, String id, Object data) {
        try {
            String source = JSON.toJSONString(data);
            UpdateRequest updateRequest = new UpdateRequest(index, type, id)
                    .doc(source, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            GetResult resp = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT).getGetResult();
        } catch (IOException e) {
            log.error("get data failed", e);
        }
        return null;
    }

    /**
     * 批量插入数据
     *
     * @param index 索引名
     * @param type  类型
     * @param data  (_id 主键, json 数据)
     */
    public void bulkInsertData(String index, String type, Map<String, Map<String, Object>> data) {
        BulkRequest request = new BulkRequest();
        data.forEach((id, source) -> {
            request.add(new IndexRequest(index, type, id).source(source));
        });
        try {
            BulkResponse bulkResponse = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    log.warn("bulk insert failed msg:{}", failure.toString());
                    continue;
                }
            }
        } catch (Exception e) {
            logger.error("bulk request failed", e);
        }
    }

    /**
     * 关闭链接
     */
    public void close() {
        try {
            if (restHighLevelClient != null) {
                synchronized (ESUtil.class) {
                    if (restHighLevelClient != null) {
                        restHighLevelClient.close();
                        restHighLevelClient = null;
                    }
                }
            }
        } catch (Exception e) {
            log.error("close client failed");
        }
    }
}