package com.neo.repository;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.neo.model.Customer;
import org.assertj.core.util.Lists;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

@RunWith(SpringRunner.class)
@SpringBootTest
public class CustomerRepositoryTest {
    @Autowired
    private CustomerRepository repository;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    private static final String INDEX = "customer";

    private static final String TYPE = "customer";

    @Test
    public void saveCustomers() {
        repository.save(new Customer("Alice", "北京", 13));
        repository.save(new Customer("Bob", "北京", 23));
        repository.save(new Customer("neo", "西安", 30));
        repository.save(new Customer("summer", "烟台", 22));
    }

    @Test
    public void fetchAllCustomers() {
        System.out.println("Customers found with findAll():");
        System.out.println("-------------------------------");
        Iterable<Customer> iterable = repository.findAll();
        for (Customer customer : iterable) {
            System.out.println(customer);
        }
    }

    @Test
    public void deleteCustomers() {
        repository.deleteAll();
//        repository.deleteByUserName("neo");
    }

    @Test
    public void updateCustomers() throws IOException {
        Customer customer = repository.findByUserName("summer");
        System.out.println(customer);
        customer.setAddress("秦皇岛");
//        repository.save(customer);
//        Customer xcustomer = repository.findByUserName("summer");
//        System.out.println(xcustomer);
        Map<String, Object> map = new HashMap<>();
        map.put("address", customer.getAddress());
        List<Object> objects = Lists.newArrayList();
        objects.add("address");
        objects.add(customer.getAddress());
//        UpdateRequest updateRequest = new UpdateRequest().index("customer").
//                type("customer").id(customer.getId()).doc(map)
//                .fetchSource(new String[]{"id", "userName"}, null);
        UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, customer.getId())
                .doc(jsonBuilder().startObject().field("address", customer.getAddress()).endObject())
                .fetchSource(true).setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .retryOnConflict(5);
        ActionRequestValidationException validate = updateRequest.validate();
//        UpdateResponse update = elasticsearchTemplate.update(
//                new UpdateQueryBuilder().withId(customer.getId())
//                        .withIndexName("customer").withType("customer")
//                        .withUpdateRequest(updateRequest).withClass(Customer.class).build());
        UpdateQuery updateQuery = new UpdateQueryBuilder().withId(customer.getId())
                .withUpdateRequest(updateRequest)
                .withClass(Customer.class).build();
        UpdateResponse update = elasticsearchTemplate.update(updateQuery);
        System.out.println(update);
    }

    @Test
    public void updateCustomers2() throws IOException {
        Customer customer = repository.findByUserName("summer");
        System.out.println(customer);
        customer.setAddress("北京市海淀区西直门");
        IndexRequest indexRequest = new IndexRequest("customer", "customer", customer.getId())
                .source(jsonBuilder()
                        .startObject()
                        .field("userName", customer.getUserName())
                        .field("address", customer.getAddress())
                        .endObject());
        UpdateRequest updateRequest = new UpdateRequest("customer", "customer", customer.getId())
                .doc(jsonBuilder()
                        .startObject()
                        .field("address", "北京市海淀区西直门")
                        .endObject())
                .upsert(indexRequest);
        UpdateResponse update = elasticsearchTemplate.update(new UpdateQueryBuilder().withUpdateRequest(updateRequest).build());
    }

    @Test
    public void fetchIndividualCustomers() {
        System.out.println("Customer found with findByUserName('summer'):");
        System.out.println("--------------------------------");
        System.out.println(repository.findByUserName("summer"));
        System.out.println("--------------------------------");
        System.out.println("Customers found with findByAddress(\"北京\"):");
        String q = "北京";
        for (Customer customer : repository.findByAddress(q)) {
            System.out.println(customer);
        }
    }

    @Test
    public void fetchPageCustomers() {
        System.out.println("Customers found with fetchPageCustomers:");
        System.out.println("-------------------------------");
        Sort sort = new Sort(Sort.Direction.DESC, "address.keyword");
        Pageable pageable = PageRequest.of(0, 10, sort);
        Page<Customer> customers = repository.findByAddress("北京", pageable);
        System.out.println("Page customers " + customers.getContent().toString());
    }

    @Test
    public void fetchPage2Customers() {
        System.out.println("Customers found with fetchPageCustomers:");
        System.out.println("-------------------------------");
        QueryBuilder customerQuery = QueryBuilders.boolQuery()
                .must(matchQuery("address", "北京"));
        Page<Customer> page = repository.search(customerQuery, PageRequest.of(0, 10));
        System.out.println("Page customers " + page.getContent().toString());
        page = repository.search(customerQuery, PageRequest.of(1, 10));
        System.out.println("Page customers " + page.getContent().toString());
    }

    @Test
    public void fetchAggregation() {
        System.out.println("Customers found with fetchAggregation:");
        System.out.println("-------------------------------");
        QueryBuilder customerQuery = QueryBuilders.boolQuery()
                .must(matchQuery("address", "北京"));
        SumAggregationBuilder sumBuilder = AggregationBuilders.sum("sumAge").field("age");
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(customerQuery)
                .addAggregation(sumBuilder)
                .build();
        Aggregations aggregations = elasticsearchTemplate.query(searchQuery, new ResultsExtractor<Aggregations>() {
            @Override
            public Aggregations extract(SearchResponse response) {
                return response.getAggregations();
            }
        });
        //转换成map集合
        Map<String, Aggregation> aggregationMap = aggregations.asMap();
        //获得对应的聚合函数的聚合子类，该聚合子类也是个map集合,里面的value就是桶Bucket，我们要获得Bucket
        InternalSum sumAge = (InternalSum) aggregationMap.get("sumAge");
        System.out.println("sum age is " + sumAge.getValue());
    }

    @Test
    public void testSomeField() {
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
// .withQuery(builder)
// .withIndices("your_index*").withTypes("your_type")
// .withPageable(PageRequest.of(0,1))
// .withSort(SortBuilders.scoreSort())
                .withSort(SortBuilders.fieldSort("age").order(SortOrder.DESC))
                .withSourceFilter(new FetchSourceFilterBuilder().withIncludes("userName").withExcludes("age").build())
                .build();
        Page<Customer> search = repository.search(searchQuery);
        String string = search.getContent().toString();
//        System.out.println(string);
        SearchQuery searchQuery1 = new NativeSearchQueryBuilder()
                .withIndices("customer", "customer")
                .withQuery(QueryBuilders.boolQuery().must(matchQuery("address", "北京"))
                        .should(rangeQuery("age").lt(25)).must(matchQuery("userName", "o"))).build();
        Page<Customer> search1 = repository.search(searchQuery1);
        System.out.println(search1.getContent().toString());
    }
}
