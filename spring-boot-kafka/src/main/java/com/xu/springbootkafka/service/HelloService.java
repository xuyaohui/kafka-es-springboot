package com.xu.springbootkafka.service;

/**
 * @author pyi
 * @date 2019-06-26
 */
public interface HelloService {

    void hello(String topic, String message);

}
