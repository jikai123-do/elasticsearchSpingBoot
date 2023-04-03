package com.iotek.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iotek.pojo.Student;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by x on 2023/4/2.
 */
@Configuration
public class ElasticConfig {

   @Value("${elasticsearch.hostip}")
    String hostip;
    @Value("${elasticsearch.port}")
    int port;


    @Bean("restHighLevelClient")
    public RestHighLevelClient  restHighLevelClient(){

     RestHighLevelClient restHighLevelClient = new RestHighLevelClient(RestClient.builder( new HttpHost(this.hostip,this.port,"http")));
     return  restHighLevelClient;

 }



}
