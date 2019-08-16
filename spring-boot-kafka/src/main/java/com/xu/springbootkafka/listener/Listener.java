package com.xu.springbootkafka.listener;

import com.alibaba.fastjson.JSONObject;
import com.xu.core.service.impl.EsClientServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * 定义专门处理消息方法
 * @author xuyaohui
 * @date 2019-08-03
 *
 * @descrption 消费信息，将数据存入es
 */
@Slf4j
@Component
public class Listener {

    @Autowired
    EsClientServiceImpl esClientService;

    /**
     *   KafkaListener注解 相当于 `KafkaMessageListenerContainer` 消费者的监听器, 方法实体部分相当于具体处理的消息内容
     * @param consumerRecord
     */
    @KafkaListener(id = "container_1", topics = {"topic1", "topic2"})
    @Async("asyncPromiseExecutor")
    public void test1(ConsumerRecord<?, ?> consumerRecord) {
        log.info("container_1要消费的信息为： "+consumerRecord.toString());

        String value = consumerRecord.value().toString();
        JSONObject jsonObject=new JSONObject();
        jsonObject.put("logText",value);
        esClientService.insertJsonValue("201908","log","1",jsonObject);
    }

}
