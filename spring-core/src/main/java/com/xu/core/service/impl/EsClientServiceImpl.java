package com.xu.core.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.xu.core.service.EsClientService;
import com.xu.core.utils.ElasticSearchUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @program : kafka-es-grafana
 * @description : 实现类
 * @author: xuyaohui
 * @create: 2019-08-03-20
 */
@Component
@Slf4j
public class EsClientServiceImpl implements EsClientService {

    @Override
    public boolean createIndex(String indexName) {
        if (!ElasticSearchUtil.checkIndexExist(indexName)) {
            if (ElasticSearchUtil.createIndex(indexName)) {
                log.info("create index success");
                return true;
            } else {
                log.info("create index false");
                return false;
            }
        } else {
            log.info("create index false, index is exists");
            return false;
        }
    }

    @Override
    public boolean insertJsonValue(String indexName, String indexType, String id, JSONObject jsonObject) {
        createIndex(indexName);
        String rid = ElasticSearchUtil.addData(indexName, indexType, id, jsonObject);
        return rid != null;
    }

    @Override
    public boolean dropIndex(String indexName) {
        try {
            log.info("delete index success");
            ElasticSearchUtil.deleteIndexIfExist(indexName);
            return true;
        }catch (IOException e){
            log.info("delete index error, error message: "+e.getMessage());
        }
        return false;
    }

}
