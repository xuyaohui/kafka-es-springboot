package com.xu.core.service;

import com.alibaba.fastjson.JSONObject;

/**
 * @program : kafka-es-grafana
 * @description : elasticsearch操作
 * @author: xuyaohui
 * @create: 2019-08-03-20
 */
public interface EsClientService {

    //创建索引
    boolean createIndex(String indexName);

    //插入json数据
    boolean  insertJsonValue(String indexName, String indexType, String id, JSONObject jsonObject);

    //删除索引
    boolean dropIndex(String indexName);
}
