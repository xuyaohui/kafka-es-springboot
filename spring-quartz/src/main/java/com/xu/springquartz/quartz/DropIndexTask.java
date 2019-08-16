package com.xu.springquartz.quartz;

import com.xu.core.service.impl.EsClientServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @program : kafka-es-grafana
 * @description : 按天删除索引
 * @author: xuyaohui
 * @create: 2019-08-04-14
 */

@Component
@Slf4j
public class DropIndexTask {

    @Autowired
    EsClientServiceImpl esClientService;

    @Scheduled(cron = "0 0 1 * * ?")
    public void dropNgLogIndex() {
        try {
            String deleteIndex = dateAdd(-7);
            boolean result = esClientService.dropIndex(deleteIndex);
            if (!result) {
                log.warn("index:{} delete fail", deleteIndex);
            } else {
                log.info("index:{} delete success", deleteIndex);
            }
        } catch (Exception e) {
            log.error("dropIndex task error,error message:{}", e.getMessage(), e);
        }
    }

    /**
     * 获得当前日期前n天的日期
     */
    private String dateAdd(int interval) {
        // 得到当前时间n天前的日期并转换成yyyyMMdd格式
        Calendar rightNow = Calendar.getInstance();
        rightNow.setTime(new Date());
        rightNow.add(Calendar.DAY_OF_YEAR, interval);
        Date date = rightNow.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return sdf.format(date);
    }
}
