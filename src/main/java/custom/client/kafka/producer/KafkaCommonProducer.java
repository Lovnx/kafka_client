package custom.client.kafka.producer;

import com.alibaba.fastjson.JSON;
import custom.client.kafka.Message.Message;
import custom.client.kafka.exception.KafkaException;
import custom.client.kafka.exception.kafkaExceptionEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.UUID;

/**
 * @program: kafka-test
 * @description: kafka普通生产者
 * @author: ZengShiLin
 * @create: 2019-07-16 18:22
 **/
@Slf4j
@Service
public class KafkaCommonProducer implements InitializingBean {

    private KafkaProducer<String, String> producer;

    private volatile boolean INITIALIZE = false;

    @Override
    public void afterPropertiesSet() throws Exception {

    }


    public KafkaCommonProducer() {
        this.init();
    }

    /**
     * 初始化生产者
     */
    private void init() {
        Properties properties = this.getProperties();
        this.producer = new KafkaProducer<>(properties);
        this.INITIALIZE = true;
    }

    private Properties getProperties() {
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
        return properties;
    }


    //TODO 同步发送

    /**
     * 同步发送（等待同步响应）
     *
     * @param message 需要发送的消息
     */
    public <T> void sendSync(Message<T> message) {
        //没有初始化不给发送
        if (!INITIALIZE) {
            throw new KafkaException(kafkaExceptionEnum.PRODUCER_THREADLOCAL_INITIALIZE_FAILURE.getValue()
                    , kafkaExceptionEnum.PRODUCER_THREADLOCAL_INITIALIZE_FAILURE.getName());
        }
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    message.getTopic(),
                    message.getKey(),
                    JSON.toJSONString(message.getValue()));
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("发送成功，metadata：" + JSON.toJSONString(metadata));
        } catch (Exception e) {
            e.printStackTrace();
            log.error("消息发送失败:{}", e);
            throw new KafkaException(kafkaExceptionEnum.PRODUCER_SEND_FAILURE.getValue(),
                    kafkaExceptionEnum.PRODUCER_SEND_FAILURE.getName());
        } finally {
            producer.flush();
        }
    }

    //TODO 异步发送


    //TODO 异步回调发送


}
