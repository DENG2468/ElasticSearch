import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class
LowLevelClientExample {

    private static final String HOST = "localhost";
    private static final int PORT = 9200;

    public static void main(String[] args) {
        // 创建HttpClient实例
        CloseableHttpClient httpClient = HttpClients.createDefault();

        // 创建HttpHost实例，指向Elasticsearch服务器
        HttpHost httpHost = new HttpHost(HOST, PORT, "http");

        // 创建索引并添加文档
        try {
            // 创建索引请求
            String indexRequest = "{\"mappings\": {\"properties\": {\"field1\": {\"type\": \"text\"}, \"field2\": {\"type\": \"integer\"}}}}";
            HttpPost createIndex = new HttpPost("/" + "my_index" + "/");
            createIndex.setEntity(new StringEntity(indexRequest, "UTF-8"));
            createIndex.setHeader("Content-type", "application/json");

            // 执行创建索引请求
            CloseableHttpResponse createIndexResponse = httpClient.execute(httpHost, createIndex);
            System.out.println("Create index response: " + EntityUtils.toString(createIndexResponse.getEntity()));

            // 索引文档请求
            String indexDocumentRequest = "{\"field1\": \"value1\", \"field2\": 2}";
            HttpPost indexDocument = new HttpPost("/" + "my_index" + "/_doc");
            indexDocument.setEntity(new StringEntity(indexDocumentRequest, "UTF-8"));
            indexDocument.setHeader("Content-type", "application/json");

            // 执行索引文档请求
            CloseableHttpResponse indexDocumentResponse = httpClient.execute(httpHost, indexDocument);
            System.out.println("Index document response: " + EntityUtils.toString(indexDocumentResponse.getEntity()));

            // 关闭HttpClient实例
            httpClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}