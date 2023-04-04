package com.iotek.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iotek.pojo.Student;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by x on 2023/4/2.
 */
@RestController
public class ElasticsearchController {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @RequestMapping(value="/addIndex",name="增加索引")
     public IndexResponse addIndex() throws IOException {
         IndexRequest indexRequest = new IndexRequest();
         indexRequest.index("elasticspringboot");
         indexRequest.id("1");
         Student student=new Student(1,"zhangsanfeng","wudang");
         String s = new ObjectMapper().writeValueAsString(student);
         indexRequest.source(s, XContentType.JSON);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("增加索引成功");
         return  indexResponse;

    }



    @RequestMapping(value="/getIndex",name="查询索引")
    public GetResponse getIndex() throws IOException {
        GetRequest getRequest = new GetRequest();
        getRequest.index("elasticspringboot");
        getRequest.id("1");

        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        Set<Map.Entry<String, DocumentField>> entries = getResponse.getFields().entrySet();
        for (Map.Entry<String, DocumentField> entry:entries){

            System.out.println(entry.getKey()+"----"+entry.getValue());

        }

        System.out.println("索引内容："+getResponse.getSourceAsString());
        return  getResponse;

    }

    @RequestMapping(value="/isIndex",name="是否为索引")
    public boolean isIndex() throws IOException {
        GetRequest getRequest = new GetRequest();
        getRequest.index("elasticspringboot");
        getRequest.id("1");

        boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);

        return  exists;

    }



    @RequestMapping(value="/updateIndex",name="更新索引")
    public UpdateResponse updateIndex() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("elasticspringboot");
        updateRequest.id("1");
        Student student=new Student(1,"zhangwuji","guangmingding");
        String s = new ObjectMapper().writeValueAsString(student);
        updateRequest.doc(s, XContentType.JSON);
        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);

        System.out.println("更新索引成功");
        return  update;

    }



    @RequestMapping(value="/deleteIndex",name="删除索引")
    public DeleteResponse deleteIndex() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest();
        deleteRequest.index("elasticspringboot");
        deleteRequest.id("1");

        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println("删除索引成功");
        return  delete;

    }



    @RequestMapping(value="/bulkAddIndex",name="批量增加索引")
    public void bulkAddIndex() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i=0;i<10;i++){
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.index("elasticspringboot");
            indexRequest.id(""+i);
            Student student=new Student(i,"zhangsanfeng"+i,"wudang"+i);
            String s = new ObjectMapper().writeValueAsString(student);
            indexRequest.source(s, XContentType.JSON);
           bulkRequest.add(indexRequest);
        }

        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest,RequestOptions.DEFAULT );

        for (BulkItemResponse bulkItemResponse:bulk){
            System.out.println(bulkItemResponse.getIndex()+"====="+bulkItemResponse.getId());

        }
        System.out.println("批量增加索引成功");


    }


    @RequestMapping(value="/multiGetIndex",name="批量获取索引")
    public void multiGetIndex() throws IOException {
        MultiGetRequest request = new MultiGetRequest();
        request.add("elasticspringboot","1");
        request.add("elasticspringboot","2");
        request.add("elasticspringboot","3");

        MultiGetResponse multiGetItemResponses = restHighLevelClient.multiGet(request, RequestOptions.DEFAULT);

        for (MultiGetItemResponse multi:multiGetItemResponses){
            System.out.println(multi.getIndex()+"====="+multi.getId()+multi.getResponse().getSourceAsString());

        }
        System.out.println("批量获取索引成功");


    }




    @RequestMapping(value="/bulkDeleteIndex",name="批量删除索引")
    public BulkResponse bulkDeleteIndex() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();

        DeleteRequest deleteRequest = new DeleteRequest();
        deleteRequest.index("elasticspringboot");
        deleteRequest.id("1");
        bulkRequest.add(deleteRequest);

        DeleteRequest deleteRequest2 = new DeleteRequest();
        deleteRequest2.index("elasticspringboot");
        deleteRequest2.id("2");
        bulkRequest.add(deleteRequest2);

        DeleteRequest deleteRequest3 = new DeleteRequest();
        deleteRequest3.index("elasticspringboot");
        deleteRequest3.id("3");
        bulkRequest.add(deleteRequest3);


        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        System.out.println("批量删除索引成功");
        return  bulk;

    }


    @RequestMapping(value="/searchAllIndex",name="获取所有索引")
    public String searchAllIndex() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();


        //搜索全部、分词搜索和非分词搜索
       // searchSourceBuilder.query(QueryBuilders.matchAllQuery());//设置搜索规则，获取当前所有索引
         searchSourceBuilder.query(QueryBuilders.matchQuery("adress","中华人民共和国"));//进行分词查询
        //searchSourceBuilder.query(QueryBuilders.termQuery("adress","华"));//不进行分词查询，完全匹配

       //分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(100);//设置分页，如果不设置默认显示10条


        //排序
        //searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.ASC));//按_socre升序排列,不指定排序时默认使用该策略排序
       // searchSourceBuilder.sort(new FieldSortBuilder("name.keyword").order(SortOrder.ASC));//按字段排序

        //源过滤
      //  searchSourceBuilder.fetchSource(false);//设置源不显示，_source字段值不显示

//        String [] include={"adress"};
//        String [] exclude={"id","name"};
//        searchSourceBuilder.fetchSource(include,exclude);//设置显示哪些列，不显示哪些列

        //设置高亮显示
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<a href>");
        highlightBuilder.postTags("</a>");
        HighlightBuilder.Field adress = new HighlightBuilder.Field("adress");//为字段创建高光
                               adress.highlighterType("unified");//设置高光类型
         highlightBuilder.field(adress);
        searchSourceBuilder.highlighter(highlightBuilder);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("elasticspringboot","myindex");//指定搜索索引名，可以指定多个
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println("获取所有索引成功");

        return search.toString();
    }

//    安装ik分词插件后测试性能--安装完以后需要重启一下es服务器， IK分词器有两种类型，分别是ik_smart 分词器和ik_max_word分词器。
//    模拟客服机器人对问题的自动识别
   @RequestMapping(value="/searchRobot",name="分词")
    public String searchRobot() throws IOException {

        String text="我要投诉618购买的手机" ;

        Request request = new Request("GET","_analyze");
        HashMap<String, Object> hashMap = new HashMap<>();
        hashMap.put("analyzer","ik_max_word");
        hashMap.put("text",text);
        request.setJsonEntity(new ObjectMapper().writeValueAsString(hashMap));


       Response response = restHighLevelClient.getLowLevelClient().performRequest(request);
       HttpEntity entity = response.getEntity();
       System.out.println( EntityUtils.toString(entity));
//       {"tokens":[{"token":"我","start_offset":0,"end_offset":1,"type":"CN_CHAR","position":0},
//           {"token":"要","start_offset":1,"end_offset":2,"type":"CN_CHAR","position":1},
//           {"token":"投诉","start_offset":2,"end_offset":4,"type":"CN_WORD","position":2},
//           {"token":"618","start_offset":4,"end_offset":7,"type":"ARABIC","position":3},
//           {"token":"购买","start_offset":7,"end_offset":9,"type":"CN_WORD","position":4},
//           {"token":"的","start_offset":9,"end_offset":10,"type":"CN_CHAR","position":5},
//           {"token":"手机","start_offset":10,"end_offset":12,"type":"CN_WORD","position":6}]}

       return  EntityUtils.toString(entity);
  }





}
