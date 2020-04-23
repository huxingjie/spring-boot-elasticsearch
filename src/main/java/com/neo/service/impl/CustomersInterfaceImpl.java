package com.neo.service.impl;

import com.alibaba.fastjson.JSON;
import com.neo.model.Customer;
import com.neo.repository.CustomerRepository;
import com.neo.service.CustomersInterface;
import com.neo.util.ESHighLevelUtil;
import com.neo.util.ESUtil;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

@Slf4j
@Service
public class CustomersInterfaceImpl implements CustomersInterface {

    Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private CustomerRepository customerRepository;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    private static final String INDEX = "customer";

    private static final String TYPE = "customer";

    @Autowired
    private ESUtil es;

    @Override
    public Page<Customer> searchCity(Integer pageNumber, Integer pageSize, String searchContent) {
        // 分页参数
        // Function Score Query
        FunctionScoreQueryBuilder functionScoreQueryBuilder = QueryBuilders.functionScoreQuery(
                QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("address", "西安")), ScoreFunctionBuilders.weightFactorFunction(1));
//                .add(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("description", searchContent)),
//                        ScoreFunctionBuilders.weightFactorFunction(100));
        // 创建搜索 DSL 查询
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withPageable(PageRequest.of(pageNumber, pageSize))
                .withQuery(functionScoreQueryBuilder).build();
        logger.info("\n searchCity(): searchContent [" + searchContent + "] \n DSL  = \n " + searchQuery.getQuery().toString());
        Page<Customer> searchPageResults = customerRepository.search(searchQuery);
        return searchPageResults;
    }

    @Override
    public Page<Customer> searchCustromer() {
        Pageable pageable = PageRequest.of(0, 10, Sort.by(Sort.Direction.DESC, "address.keyword"));
        Customer customer = Customer.builder().address("北京").build();
        Page<Customer> customers = customerRepository.findByAddress("北京", pageable);
//        Page<Customer> customers = customerRepository.findByAddressLike("北京", pageable);
        return customers;
    }

    /**
     * "address": {
     * "type": "text",
     * fields": {
     * "keyword": {
     * "type": "keyword",
     * "ignore_above": 256
     * }
     * }
     * },
     * 但为了保留对这些字段做精确查询以及聚合的能力，又同时对它们做了keyword类型的映射，作为该字段的fields属性写到_mapping中
     * must条件用于多条件筛选，包括多品牌等，还要可售状态的商品；should条件用于全文关键词搜索，
     *
     * @param pageNumber
     * @param pageSize
     * @param searchContent
     * @return
     */
    @Override
    public Page<Customer> searchCustromer(Integer pageNumber, Integer pageSize, String searchContent) {
        log.info("es search param:{}", searchContent);
        SearchQuery searchQuery1 = new NativeSearchQueryBuilder()
                .withIndices("customer", "customer")
                //termQuery需要加.keyword进行精确匹配
                .withQuery(QueryBuilders.boolQuery().must(rangeQuery("age").lt(30).includeUpper(false))
                        .must(QueryBuilders.boolQuery()
                                .should(matchQuery("address.keyword", "北京"))
                                .should(matchQuery("address.keyword", "西安")))
                )
                .withSort(SortBuilders.fieldSort("age").order(SortOrder.DESC))
//                .withPageable(PageRequest.of(1, 1))
                .build();
        Page<Customer> search1 = customerRepository.search(searchQuery1);
        Customer customer = Customer.builder().address("西").build();
        IndexQuery index = new IndexQueryBuilder().withId("1").withObject(customer).build();
        return search1;
    }

    @Override
    public List<Customer> searchCustromerAndOr() {
        // a =1 and (b =1 or b=2) 查询
        SearchQuery searchQuery2 = new NativeSearchQueryBuilder().withQuery(
                QueryBuilders.boolQuery()
                        .must(matchQuery("address.keyword", "北京"))
                        .must(QueryBuilders.boolQuery().should(matchQuery("userName.keyword", "Bob"))
                                .should(matchQuery("userName.keyword", "neo")))
        ).build();
        List<Customer> content = customerRepository.search(searchQuery2).getContent();
        return content;
    }

    @Override
    public boolean updateCustomer() throws Exception {
        Customer customer = customerRepository.findByUserName("summer");
        System.out.println(customer);
        customer.setAddress("秦皇岛`1");
//        es.update(INDEX, TYPE, customer.getId(), customer);
//        ESHighLevelUtil esHighLevelUtil = new ESHighLevelUtil(new String[]{"localhost:9200"});
//        esHighLevelUtil.updateData(INDEX, TYPE, customer.getId(), customer);
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.doc(JSON.toJSONString(customer), XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        UpdateQuery updateQuery = new UpdateQuery();
        updateQuery.setId(customer.getId());
        updateQuery.setIndexName(INDEX);
        updateQuery.setType(TYPE);
        updateQuery.setUpdateRequest(updateRequest);
        GetResult getResult = elasticsearchTemplate.update(updateQuery).getGetResult();
//        return getResult != null;
        return false;
    }
}
