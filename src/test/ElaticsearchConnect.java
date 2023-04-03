
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by x on 2023/4/2.
 */
public class ElaticsearchConnect {

    public static void main(String[] args) throws IOException {
        //测试连接、建立索引
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(RestClient.builder( new HttpHost("localhost",9200,"http")));

        IndexRequest indexRequest = new IndexRequest();
       indexRequest.id("1"); //设置索引id
       indexRequest.index("user");//设置索引名


        HashMap<String, Object> stringObjectHashMap = new HashMap<String, Object>();
        stringObjectHashMap.put("name","zhangsan");
        stringObjectHashMap.put("age",15);
        stringObjectHashMap.put("id",1);
        indexRequest.source(stringObjectHashMap);

        IndexResponse index = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);

           System.out.println(index.getIndex());
           System.out.println(index.status().getStatus());
           System.out.println("连接成功");
           restHighLevelClient.close();


    }



}
