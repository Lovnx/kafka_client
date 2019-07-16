package custom.client.kafka.producer;

import custom.client.kafka.config.KafkaProducerConfig;
import custom.client.kafka.exception.KafkaException;
import custom.client.kafka.exception.kafkaExceptionEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.Semaphore;

/**
 * @program: kafka-test
 * @description: 生成者对象池 TODO
 * @author: ZengShiLin
 * @create: 2019-07-09 11:26
 **/
@Slf4j
public class ProducerPool {

    /**
     * 对象池
     */
    private Vector<KafkaProducer<String, String>> producers;

    /**
     * 信号量
     */
    private Semaphore objectSemaphore;


    ProducerPool(int size, KafkaProducerConfig producerConfig) {
        try {
            producers = new Vector<>(size);
            Properties properties = MyKafkaProducer.properties(producerConfig);
            for (int i = 0; i < size; i++) {
                //创建生成者对象
                producers.add(new KafkaProducer<>(properties));
            }
            objectSemaphore = new Semaphore(size);
        } catch (Exception e) {
            log.error("生成者池初始化失败:{}", e);
            throw new KafkaException(kafkaExceptionEnum.PRODUCER_POOL_INITIALIZE_FAILURE.getValue()
                    , kafkaExceptionEnum.PRODUCER_POOL_INITIALIZE_FAILURE.getName());
        }
    }

    //TODO 使用对象池，并且编程式执行

}
