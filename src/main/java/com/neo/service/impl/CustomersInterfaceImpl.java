package com.neo.service.impl;

import com.neo.model.Customer;
import com.neo.repository.CustomerRepository;
import com.neo.service.CustomersInterface;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.stereotype.Service;

import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

@Slf4j
@Service
public class CustomersInterfaceImpl implements CustomersInterface {

    Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private CustomerRepository customerRepository;

    @Override
    public List<Customer> searchCity(Integer pageNumber, Integer pageSize, String searchContent) {
/*        // 分页参数
        Pageable pageable = new PageRequest(pageNumber, pageSize);

        // Function Score Query
        FunctionScoreQueryBuilder functionScoreQueryBuilder = QueryBuilders.functionScoreQuery()
                .add(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("cityname", searchContent)),
                        ScoreFunctionBuilders.weightFactorFunction(1000))
                .add(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("description", searchContent)),
                        ScoreFunctionBuilders.weightFactorFunction(100));

        // 创建搜索 DSL 查询
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withPageable(pageable)
                .withQuery(functionScoreQueryBuilder).build();

       logger.info("\n searchCity(): searchContent [" + searchContent + "] \n DSL  = \n " + searchQuery.getQuery().toString());

        Page<Customer> searchPageResults = customerRepository.search(searchQuery);
        return searchPageResults.getContent();
        */
        return null;
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
}
