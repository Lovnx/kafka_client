package test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @program: kafka-test
 * @description:
 * @author: ZengShiLin
 * @create: 2019-07-16 18:17
 **/
public class ProductTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        //是否等待所有broker响应
        properties.put("acks", "all");
        //broker 服务器集群
        properties.put("bootstrap.servers", "kafka-service:9092,kafka-service2:9092,kafka-service3:9092");
        //健序列化器
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //值序列化器
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //是否开启幂等
        properties.put("enable.idempotence", true);
        //client ID
        properties.put("client.id", "FINANCE_WEB_CLIENT_ID_" + UUID.randomUUID().toString());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int index = 0; index < 5; index++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic12", UUID.randomUUID().toString(), "排查测试数据" + index);
            Future<RecordMetadata> future = producer.send(record);
            future.get();
            System.out.println("发送");
        }
    }
}
