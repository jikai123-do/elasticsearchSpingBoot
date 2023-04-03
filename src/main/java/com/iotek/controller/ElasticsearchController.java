package com.iotek.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iotek.pojo.Student;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
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


}
