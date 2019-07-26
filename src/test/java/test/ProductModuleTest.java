package test;

import custom.client.kafka.config.KafkaProducerConfig;
import custom.client.kafka.message.Message;
import custom.client.kafka.producer.MyKafkaProducer;

import java.util.UUID;

/**
 * @program: kafka-test
 * @description:
 * @author: ZengShiLin
 * @create: 2019-07-09 12:25
 **/
public class ProductModuleTest {

    static MyKafkaProducer producer = new MyKafkaProducer(KafkaProducerConfig.builder()
            .bootstrapServers("kafka-service:9092,kafka-service2:9092,kafka-service3:9092")
            .acks("all")
            .enableTransactional(false)
            .enableIdempotence(true)
            .appName("finance-web")
            .build());

    public static void main(String[] args) throws Exception {
        Message<String> message = new Message<>(UUID.randomUUID().toString(), "zsl-finance-service-test", "测试数据【新新1】");
        //同步发送
        producer.sendSync(message);
    }


}
