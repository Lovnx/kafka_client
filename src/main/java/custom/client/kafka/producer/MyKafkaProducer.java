package custom.client.kafka.producer;

import custom.client.kafka.config.KafkaProducerConfig;
import custom.client.kafka.exception.KafkaException;
import custom.client.kafka.exception.kafkaExceptionEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

/**
 * @program: kafka-test
 * @description: kafka生产者（支持池化和，ThreadLocal）
 * @author: ZengShiLin
 * @create: 2019-07-09 09:06
 **/
@Slf4j
public class MyKafkaProducer implements InitializingBean {

    /**
     * 生产者配置
     */
    @Autowired(required = false)
    private KafkaProducerConfig producerConfig;

    /**
     * 线程本地生产者(双重)
     */
    private static ThreadLocal<KafkaProducer<String, String>> PRODUCER_THREADLOCAL;

    /**
     * 是否已经初始化（volatile 增强线程可见性）
     */
    private volatile static boolean INITIALIZE = false;

    @Override
    public void afterPropertiesSet() throws Exception {
        //双重锁校验
        synchronized (MyKafkaProducer.class) {
            Properties properties = this.properties(this.producerConfig);
            PRODUCER_THREADLOCAL = new ThreadLocal<>();
            PRODUCER_THREADLOCAL.set(new KafkaProducer<>(properties));
            INITIALIZE = true;
        }
        log.info("生产者初始化完成");
    }

    /**
     * 服务关闭
     */
    private synchronized void shutdown() {
        INITIALIZE = false;
    }


    /**
     * 配置内容
     *
     * @return 配置内容
     */
    private Properties properties(KafkaProducerConfig producerConfig) {
        if (null == producerConfig) {
            log.error("生产者配置为空");
            throw new KafkaException(kafkaExceptionEnum.PRODUCER_CONFIGURATION_IS_EMPTY.getValue()
                    , kafkaExceptionEnum.PRODUCER_CONFIGURATION_IS_EMPTY.getName());
        }
        Properties properties = new Properties();
        //是否等待所有broker响应
        properties.put(ProducerConfig.ACKS_CONFIG, Optional.ofNullable(producerConfig.getAcks()).orElse("all"));
        //broker 服务器集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerConfig.getBootstrapServers());
        //健序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //值序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //是否开启幂等
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        //事务 ID
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
        //client ID
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "ProducerTranscationnalExample2");
        //事务级别
        properties.put("isolation.level", "read_committed");
        return properties;
    }


}
