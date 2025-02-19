import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.UnsupportedEncodingException;

public class HttpComponentsExample {

    private static final String HOST = "localhost";
    private static final int PORT = 9200;
    private static final String INDEX_URL = "http://" + HOST + ":" + PORT + "/my_index/_doc";

    public static void main(String[] args) throws UnsupportedEncodingException {
        // 创建HttpClient实例
        HttpClient httpClient = HttpClients.createDefault();

        // 构建HTTP POST请求
        HttpPost postRequest = new HttpPost(INDEX_URL);
        postRequest.setHeader("Content-Type", "application/json");
        postRequest.setHeader("Accept", "application/json");

        // JSON数据
        String jsonDocument = "{\"field1\": \"value1\", \"field2\": 2}";

        // 设置请求体
        postRequest.setEntity(new StringEntity(jsonDocument));

        try {
            // 执行请求
            HttpResponse response = httpClient.execute(postRequest);
            HttpEntity responseEntity = response.getEntity();

            // 打印响应内容
            if (responseEntity != null) {
                String result = EntityUtils.toString(responseEntity);
                System.out.println("Response: " + result);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 释放资源
            try {
                httpClient.getConnectionManager().shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
