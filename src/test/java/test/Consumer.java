package test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import custom.client.kafka.message.Message;
import custom.client.kafka.consumer.DefaultTopicMessageExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @program: kafka-test
 * @description:
 * @author: ZengShiLin
 * @create: 2019-07-17 14:24
 **/
@Slf4j
@Component
public class Consumer extends DefaultTopicMessageExecutor {
    @Override
    public String getTopic() {
        return "test-topic12";
    }

    @Override
    public boolean execute(Message message) {
        log.info("收到消息：{}", message);
        System.out.println("消费到消息:" + JSON.toJSONString(message));
        return true;
    }

    @Override
    public TypeReference getTypeReference() {
        return new TypeReference<String>() {
        };
    }
}
