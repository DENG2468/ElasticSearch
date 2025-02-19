import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.apache.http.HttpHost;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.SearchHit;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GetDocumentExample {

    private static final String HOST = "localhost";
    private static final int PORT = 9200;

    public static void main(String[] args) {
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();
        RestHighLevelClient client = new RestHighLevelClient(restClient);
        try {
            // 创建索引
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("my_index2");
            Map<String, Object> settings = new HashMap<>();
            settings.put("number_of_shards", 1);
            settings.put("number_of_replicas", 0);
            Map<String, Object> properties = new HashMap<>();
            properties.put("field1", Collections.singletonMap("type", "text"));
            properties.put("field2", Collections.singletonMap("type", "integer"));
            Map<String, Object> mappings = new HashMap<>();
            mappings.put("properties", properties);
            createIndexRequest.settings(settings).mappings();

            String jsonString = "{ \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 }, \"mappings\": { \"_doc\": { \"properties\": { \"field1\": { \"type\": \"text\" }, \"field2\": { \"type\": \"integer\" } } } } }";
            HttpEntity entity = new StringEntity(jsonString, ContentType.APPLICATION_JSON);
            Response response = restClient.performRequest(
                    "PUT", "/my_index2", Collections.singletonMap("pretty", "true"),
                    entity);
            Map<String, Object> responseMap = entityAsMap(response);
            System.out.println("Index created with status: " + responseMap.get("acknowledged"));

            // 索引文档
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("field1", "value1");
            jsonMap.put("field2", 2);
            IndexRequest indexRequest = new IndexRequest("my_index", "1");
            indexRequest.source(jsonMap);
            IndexResponse indexResponse = client.index(indexRequest);
            System.out.println("Document indexed with status: " + indexResponse.status());

            // 获取文档
            GetRequest getRequest = new GetRequest();
            GetResponse getResponse = client.get(getRequest);
            System.out.println("Document retrieved: " + getResponse.getSourceAsString());

            // 搜索文档
            SearchRequest searchRequest = new SearchRequest("my_index");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchQuery("field1", "value1"));
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(searchRequest);
            System.out.println("Search response: " + searchResponse.toString());

            // 打印搜索结果
            for (SearchHit hit : searchResponse.getHits()) {
                System.out.println(hit.getSourceAsString());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Map<String, Object> entityAsMap(Response response) throws IOException{
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            String jsonString = EntityUtils.toString(entity);
            // 手动解析 JSON 字符串为 Map
            return (Map<String, Object>) new JSONObject(jsonString);
        } else {
            return Collections.emptyMap();
        }
    }
}
