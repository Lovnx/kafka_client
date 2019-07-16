package test;

import com.google.common.collect.Maps;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import test.timer.HashedWheelTimer;
import test.timer.Timer;
import test.timer.TimerTask;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @program: kafka_test
 * @description:
 * @author: ZengShiLin
 * @create: 2019-06-24 17:55
 **/
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:*spring-service.xml")
public class MessageProductTest {


    @Test
    public void test() {
        MessageProductTest test = new MessageProductTest();
        test.send();
    }

    @Test
    public void time() throws InterruptedException {
        final Timer timer = new HashedWheelTimer(Executors.defaultThreadFactory(), 5, TimeUnit.SECONDS, 2);
        TimerTask task1 = timeout -> {
            System.out.println("task 1 will run per 5 seconds ");
            //timer.newTimeout(this, 5, TimeUnit.SECONDS);//结束时候再次注册
        };
        timer.newTimeout(task1, 5, TimeUnit.SECONDS);
        Thread.sleep(40000);
    }

    public void send() {
        KafkaTemplate<Integer, String> template = new KafkaTemplate<>(this.producerFactory());
        template.send("test-topic", 123, "测试数据1");
    }


    private Map<String, Object> properties2() {
        Map<String, Object> properties = Maps.newHashMap();
        properties.put("acks", "all");
        properties.put("bootstrap.servers", "service1:9092,service2:9092,service3:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //事务ID
        //properties.put("transactional.id", "test-transactional");
        //客户端ID
        //properties.put("client.id", "ProducerTranscationnalExample");
        //开启幂等
        //properties.put("enable.idempotence", true);
        return properties;
    }


    @Bean
    public ProducerFactory<Integer, String> producerFactory() {
        DefaultKafkaProducerFactory<Integer, String> factory = new DefaultKafkaProducerFactory<>(this.properties2());
        factory.transactionCapable();
        factory.setTransactionIdPrefix("tran-");
        return factory;
    }

    @Bean
    public KafkaTransactionManager<String, String> transactionManager(ProducerFactory producerFactory) {
        KafkaTransactionManager<String, String> manager = new KafkaTransactionManager<>(producerFactory);
        return manager;
    }


}
