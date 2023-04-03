package com.iotek.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iotek.pojo.Student;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
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



    @RequestMapping(value="/deleteIndex",name="更新索引")
    public DeleteResponse deleteIndex() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest();
        deleteRequest.index("elasticspringboot");
        deleteRequest.id("1");

        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println("删除索引成功");
        return  delete;

    }


}
