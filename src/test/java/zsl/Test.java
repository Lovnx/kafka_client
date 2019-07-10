package zsl;

import custom.client.kafka.Message.Message;
import custom.client.kafka.config.KafkaProducerConfig;
import custom.client.kafka.producer.MyKafkaProducer;

import java.util.UUID;

/**
 * @program: kafka-test
 * @description:
 * @author: ZengShiLin
 * @create: 2019-07-09 12:25
 **/
public class Test {

    public static void main(String[] args) throws Exception {
        MyKafkaProducer producer = new MyKafkaProducer(KafkaProducerConfig.builder()
                .acks("all")
                .bootstrapServers("kafka-service:9092,kafka-service2:9092,kafka-service3:9092")
                .enableIdempotence(true)
                .enableTransactional(true)
                .appName("finance-web")
                .isolationLevel("read_committed")
                .build());

        producer.sendSync(Message.builder()
                .value("123123")
                .key(UUID.randomUUID().toString())
                .topic("test-topice12")
                .build());
    }
}
