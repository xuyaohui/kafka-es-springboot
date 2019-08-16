package com.xu.core.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xu.core.annotation.ElasticId;
import com.xu.core.annotation.ElasticIndex;
import com.xu.core.vo.TbEsBaseVO;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;


/*** @Title: ElasticsearchUtil* @Description: 工具类* @author chy* @date 2018/4/24 15:40*/
@Component
@Slf4j
public class ElasticSearchUtil {

    @Autowired
    private RestHighLevelClient getRHLClient;
    private static RestHighLevelClient restHLclient;
    private static RestClient restClient;
    private static ObjectMapper mapper = new ObjectMapper();

    /**
     * @PostContruct是spring框架的注解 * spring容器初始化的时候执行该方法
     */
    @PostConstruct
    public void init() {
        restHLclient = this.getRHLClient;
        restClient = this.getRHLClient.getLowLevelClient();
    }

    /**
     * 创建索引     *     * @param index     * @return
     */
    public static boolean createIndex(String index) {
        index = index.toLowerCase();
        //index名必须全小写，否则报错
        CreateIndexRequest request = new CreateIndexRequest(index);
        try {
            if(checkIndexExist(index)){
                throw new Exception("index 存在");
            }
            CreateIndexResponse indexResponse = restHLclient.indices().create(request);
            if (indexResponse.isAcknowledged()) {
                log.info("创建索引成功");
            } else {
                log.info("创建索引失败");
            }
            return indexResponse.isAcknowledged();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static boolean createIndex( CreateIndexRequest request) {
        try {
            if(checkIndexExist(  request.index())){
                throw new Exception("index 存在");
            }

            CreateIndexResponse indexResponse = restHLclient.indices().create(request);
            if (indexResponse.isAcknowledged()) {
                log.info("创建索引成功");
            } else {
                log.info("创建索引失败");
            }
            return indexResponse.isAcknowledged();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


    /**
     * 检查索引     * @param index     * @return     * @throws IOException
     */
    public static boolean checkIndexExist(String index){
        Response response = null;
        try {
            response = restClient.performRequest("HEAD", index);
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean exist = response.getStatusLine().getReasonPhrase().equals("OK");
        return exist;
    }
    /**
     * 删除索引
     */
    public static boolean deleteIndexIfExist(String index) throws IOException {
        if(checkIndexExist(index)){
            Response response = restClient.performRequest("DELETE", index);
            boolean exist = response.getStatusLine().getReasonPhrase().equals("OK");
            return exist;
        }
        return false;
    }
    /**
     * 删除索引
     */
    public static boolean deleteIndex(String index) {

        boolean exist = false;
        try {
            if (!checkIndexExist(index)) {
                throw new Exception("index 不存在");
            }
            Response response = restClient.performRequest("DELETE", index);
            exist = response.getStatusLine().getReasonPhrase().equals("OK");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return exist;

    }

    /**
     * 插入数据     * @param index     * @param type     * @param object     * @return
     */
    public static String addData(String index, String type, String id, JSONObject object) {
        IndexRequest indexRequest = new IndexRequest(index, type, id);
        try {
            indexRequest.source(mapper.writeValueAsString(object), XContentType.JSON);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return addIndexRequest(indexRequest);
    }

    public static <T> String addObject(T object) {
        if (null == object) {
            return null;
        }
        TbEsBaseVO tbEsBaseVO = null;
            Map map = null;
        try {
            tbEsBaseVO = getBaseVO(object);
            map = objectToMap(object, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        IndexRequest indexRequest = new IndexRequest(tbEsBaseVO.getIndices(), tbEsBaseVO.getType(), tbEsBaseVO.getId()).source(map);
       return addIndexRequest(indexRequest);
    }

    public static String addIndexRequest(IndexRequest indexRequest) {
        if (null == indexRequest) {
            return null;
        }
        try {
            IndexResponse indexResponse = restHLclient.index(indexRequest);
            if (null != indexResponse)
                return indexResponse.getId();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    private static <T> TbEsBaseVO getBaseVO(T t) throws Exception {
        TbEsBaseVO tbEsBaseVO = new TbEsBaseVO();
        //获取注解的值
        tbEsBaseVO.setIndices(t.getClass().getAnnotation(ElasticIndex.class).index());
        if (StringUtils.isEmpty(tbEsBaseVO.getIndices())) {
            throw new Exception("index为空");
        }
        tbEsBaseVO.setType(t.getClass().getAnnotation(ElasticIndex.class).type());
        if (StringUtils.isEmpty(tbEsBaseVO.getIndices())) {
            throw new Exception("type为空");
        }
        //获取它的所有字段
        List<Field> fieldList = Arrays.asList(t.getClass().getDeclaredFields());
        Field field;
        for (int i = 0; i < fieldList.size(); i++) {
            field = fieldList.get(i);
            //是否使用了这个注解
            if (field.isAnnotationPresent(ElasticId.class)) {
                String name = field.getName();
                Method[] methods = t.getClass().getMethods();
                for (int j = 0; j < methods.length; j++) {
                    //获取它的get方法
                    if (("get" + name).toLowerCase().equals(methods[j].getName().toLowerCase())) {
                        tbEsBaseVO.setId(methods[j].invoke(t).toString());
                        break;
                    }
                }
                break;
            }
        }
        if (StringUtils.isEmpty(tbEsBaseVO.getId())) {
            throw new Exception("对象id为空");
        }
        return tbEsBaseVO;
    }




    public static Map objectToMap(Object object) throws Exception {
        return objectToMap(object, true);
    }

    public static Map objectToMap(Object object, boolean isNeedNull) throws Exception {
        if (null == object) {
            return null;
        }
        if (isNeedNull) {
            return JSON.parseObject(JSONObject.toJSONString(object, SerializerFeature.WRITE_MAP_NULL_FEATURES), Map.class);
        } else {
            return JSON.parseObject(JSONObject.toJSONString(object), Map.class);
        }
    }


    public static <T> T searchById(String id, Class<T> tClass) throws  Exception{
        ApplicationContext applicationContext=  SpringUtil.getApplicationContext();
        RestHighLevelClient asd = (RestHighLevelClient) applicationContext.getBean("getRestHighLevelClient");
        if(null == id){
            return null;
        }

        GetRequest getRequest = new GetRequest(tClass.getAnnotation(ElasticIndex.class).index(), tClass.getAnnotation(ElasticIndex.class).type(), id);
        GetResponse response = restHLclient.get(getRequest);
        //如果查询为空
        if(null == response.getSourceAsString()){
            return null;
        }
        return JSON.parseObject(response.getSourceAsString(), tClass);
    }


    /**
     * 获取低水平客户端     * @return
     */
    public static RestClient getLowLevelClient() {
        return restClient;
    }

    public static HttpEntity queryAll(String indexName, String esType){
        try {
            HttpEntity entity = new NStringEntity("{ \"query\": { \"match_all\": {}}}", ContentType.APPLICATION_JSON);
            String endPoint = "/" + indexName + "/" + esType + "/_search";
            Response response =  queryMatch( endPoint,  entity);
            return response.getEntity();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static Response queryMatch(String endPoint, HttpEntity entity) throws IOException {
        return ElasticSearchUtil.getLowLevelClient().performRequest("POST", endPoint, Collections.<String, String>emptyMap(), entity);
    }

    public static Response queryMatch(String endPoint, String argument) throws IOException {
        HttpEntity entity = new NStringEntity(argument, ContentType.APPLICATION_JSON);
        return ElasticSearchUtil.getLowLevelClient().performRequest("POST", endPoint, Collections.<String, String>emptyMap(), entity);
    }

    public static Response queryMatch(String endPoint, IndexRequest indexRequest) throws IOException {
        String source = indexRequest.source().utf8ToString();
        HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
        return ElasticSearchUtil.getLowLevelClient().performRequest("POST", endPoint, Collections.<String, String>emptyMap(), entity);
    }

    public static Response queryMatch(String endPoint, XContentBuilder builder) throws IOException {
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.source(builder);
        String source = indexRequest.source().utf8ToString();
        HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
        return ElasticSearchUtil.getLowLevelClient().performRequest("POST", endPoint, Collections.<String, String>emptyMap(), entity);
    }




}


