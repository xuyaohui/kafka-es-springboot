package com.xu.springbootes.controller;
import com.alibaba.fastjson.JSONObject;
import com.xu.springbootes.entity.GoodInfo;
import com.xu.core.service.impl.EsClientServiceImpl;
import com.xu.core.utils.ElasticSearchUtil;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;

@RestController
@RequestMapping("/es")
public class ESController {
    private static final Logger logger = LoggerFactory.getLogger(ESController.class);
    /**
     * 测试索引
     */
    private String indexName = "xuyaohui";
    /**
     * 类型
     */
    private String esType = "normal";

    @Autowired
    EsClientServiceImpl esClientService;

    /**
     * 首页     * @return
     */
    @RequestMapping("/index")
    public String index() {
        return "index";
    }

    /**
     * http://localhost:8080/es/createIndex     * 创建索引     * @param request     * @param response     * @return
     */
    @RequestMapping("/createIndex/{indexName}")
    public String createIndex(@PathVariable String indexName) throws IOException {
        return esClientService.createIndex(indexName)? "create index success" : "create index false";
    }

    /**
     * 插入记录     * http://localhost:8080/es/addData     * @return
     */
    @RequestMapping("/addData/{id}")
    @ResponseBody
    public String addData(@PathVariable String id) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id",String.valueOf(new Date().getTime()));
        jsonObject.put("age", 29);
        jsonObject.put("name", "liming");
        jsonObject.put("date", new Date());
        id = ElasticSearchUtil.addData(indexName, esType, id, jsonObject);
        if (!id.isEmpty()) {
            return "插入成功:"+id;
        } else {
            return "插入失败";
        }
    }


    @RequestMapping("/addObj/{id}")
    @ResponseBody
    public String addObj(@PathVariable Long id) {
       GoodInfo gi = new GoodInfo();
       gi.setId(id);
       gi.setName("momo");
       gi.setDescription("默默是个傻瓜");
       String  rid = ElasticSearchUtil.addObject(gi);
        if (!rid.isEmpty()) {
            return "插入成功:"+rid;
        } else {
            return "插入失败";
        }
    }

    @RequestMapping("/findObj/{id}")
    @ResponseBody
    public Object findObj(@PathVariable String id) throws Exception {
      return  ElasticSearchUtil.searchById(id,GoodInfo.class);

    }

    /**
     * 查询所有     * @return
     */
    @RequestMapping("/queryAll")
    @ResponseBody
    public String queryAll(String indexName,String esType) throws IOException {
        return EntityUtils.toString(ElasticSearchUtil.queryAll(indexName,esType));
    }

    /**
     * 根据条件查询     * @return
     */
    @RequestMapping("/queryByMatch")
    @ResponseBody
    public String queryByMatch(String indexName,String esType,@RequestParam(defaultValue = "") String find) {
        try {
            String endPoint = "/" + indexName + "/" + esType + "/_search";
            IndexRequest indexRequest = new IndexRequest();
            XContentBuilder builder;
            try {
                builder = JsonXContent.contentBuilder()
                        .startObject()
                            .startObject("query")
                               .startObject("match")
                                   .field("name.keyword", find)
                               .endObject()
                            .endObject()
                        .endObject();
                indexRequest.source(builder);
            } catch (IOException e) {
                e.printStackTrace();
            }
            String source = indexRequest.source().utf8ToString();
            logger.info("source---->" + source);
            HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
            Response response = queryMatch(endPoint, entity);
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "查询数据出错";
    }

    /**
     * 复合查询     * @return
     */
    @RequestMapping("/queryByCompound")
    @ResponseBody
    public String queryByCompound() {
        try {
            String endPoint = "/" + indexName + "/" + esType + "/_search";
            IndexRequest indexRequest = new IndexRequest();
            XContentBuilder builder;
            try {                /**                 * 查询名字等于 liming                 * 并且年龄在30和35之间                 */
                builder = JsonXContent.contentBuilder().startObject().startObject("query").startObject("bool").startObject("must").startObject("match").field("name.keyword", "liming").endObject().endObject().startObject("filter").startObject("range").startObject("age").field("gte", "30").field("lte", "35").endObject().endObject().endObject().endObject().endObject().endObject();
                indexRequest.source(builder);
            } catch (IOException e) {
                e.printStackTrace();
            }
            String source = indexRequest.source().utf8ToString();
            logger.info("source---->" + source);
            HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
            Response response = queryMatch(endPoint, entity);
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "查询数据出错";
    }

    private Response queryMatch(String endPoint, HttpEntity entity) throws IOException {
        return ElasticSearchUtil.getLowLevelClient().performRequest("POST", endPoint, Collections.<String, String>emptyMap(), entity);
    }

    /**
     * 删除查询的数据     * @return
     */
    @RequestMapping("/delByQuery")
    @ResponseBody
    public String delByQuery() {
        String deleteText = "chy";
        String endPoint = "/" + indexName + "/" + esType + "/_delete_by_query";         /**         * 删除条件         */
        IndexRequest indexRequest = new IndexRequest();
        XContentBuilder builder;
        try {
            builder = JsonXContent.contentBuilder().startObject().startObject("query").startObject("term")
                    //name中包含deleteText
                    .field("name.keyword", deleteText).endObject().endObject().endObject();
            indexRequest.source(builder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String source = indexRequest.source().utf8ToString();
        HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
        try {
            Response response = queryMatch(endPoint, entity);
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "删除错误";
    }

    /**
     * 演示聚合统计     * @return
     */
    @RequestMapping("/aggregation")
    @ResponseBody
    public String aggregation() {
        try {
            String endPoint = "/" + indexName + "/" + esType + "/_search";
            IndexRequest indexRequest = new IndexRequest();
            XContentBuilder builder;
            try {
                builder = JsonXContent.contentBuilder().startObject().startObject("aggs").startObject("名称分组结果").startObject("terms").field("field", "name.keyword").startArray("order").startObject().field("_count", "asc").endObject().endArray().endObject().endObject().endObject().endObject();
                indexRequest.source(builder);
            } catch (IOException e) {
                e.printStackTrace();
            }
            String source = indexRequest.source().utf8ToString();
            logger.info("source---->" + source);
            HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
            Response response = queryMatch(endPoint, entity);
            return EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "查询数据出错";
    }
}


