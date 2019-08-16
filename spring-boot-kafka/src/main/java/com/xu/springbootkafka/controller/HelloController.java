package com.xu.springbootkafka.controller;

import com.xu.springbootkafka.service.impl.HelloServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author pyi
 * @date 2019-06-26
 */
@RestController
@RequestMapping("hello")
@Slf4j
public class HelloController {

    @Resource
    private HelloServiceImpl helloService;

    /**
     * 这里我们监听请求携带主题和信息, 然后想服务器发送消息供消费者消费
     * @param topic 主题
     * @param message 信息
     * @return String
     */
    @GetMapping("hello/{topic}/{message}")
    public String hello(@PathVariable String topic, @PathVariable String message) {
        log.info("Topic = {}, Message = {}", topic, message);
        helloService.hello(topic, message);
        return "success";
    }

}
