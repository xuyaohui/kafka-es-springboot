package com.xu.springbootkafka.service.impl;

import com.xu.springbootkafka.service.HelloService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import javax.annotation.Resource;

/**
 * @author pyi
 * @date 2019-06-26
 */
@Service
@Slf4j
public class HelloServiceImpl implements HelloService {

    @Resource
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Override
    public void hello(String topic, String message) {
        // 这里如果我们忽略消息是否发送成功(生产者 acks = 0)的时候就不用处理返回值, 增强系统的吞吐量, 但是消息可能会丢失
        handlerSendRecord(kafkaTemplate.send(topic, message));
    }

    private void handlerSendRecord(ListenableFuture<SendResult<Integer, String>> resultListenableFuture) {
        try {
            //这里我们可以获取到生产者消息是否提交成功
            SendResult<Integer, String> integerStringSendResult = resultListenableFuture.get();
            RecordMetadata recordMetadata = integerStringSendResult.getRecordMetadata();
            // 打印消息被存储到哪个分区, 当前偏移量是多少
            log.info("partition = " + recordMetadata.partition());
            log.info("offset = " + recordMetadata.offset());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
