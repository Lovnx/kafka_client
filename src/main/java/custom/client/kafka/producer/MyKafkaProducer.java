package custom.client.kafka.producer;

import com.alibaba.fastjson.JSON;
import custom.client.kafka.Message.Message;
import custom.client.kafka.config.KafkaProducerConfig;
import custom.client.kafka.exception.KafkaException;
import custom.client.kafka.exception.kafkaExceptionEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * @program: kafka-test
 * @description: kafka生产者（支持池化和，ThreadLocal） KafkaProducer 是线程安全，但是事务应该是ThreadLocal的
 * @author: ZengShiLin
 * @create: 2019-07-09 09:06
 **/
@Slf4j
@Service
public class MyKafkaProducer implements InitializingBean {

    /**
     * 生产者配置
     */
    @Autowired(required = false)
    private KafkaProducerConfig producerConfig;

    /**
     * 线程本地生产者  使用ThreadLocal
     */
    private static ThreadLocal<KafkaProducer<String, String>> PRODUCER_THREADLOCAL;

    /**
     * 是否已经初始化（volatile 增强可见性）
     */
    private volatile static boolean INITIALIZE = false;

    @Override
    public void afterPropertiesSet() {
        //双重锁校验
        if (null == PRODUCER_THREADLOCAL || null == PRODUCER_THREADLOCAL.get()) {
            synchronized (MyKafkaProducer.class) {
                Properties properties = MyKafkaProducer.properties(this.producerConfig);
                KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
                if (this.producerConfig.isEnableTransactional()) {
                    producer.initTransactions();
                }
                //创建生产者
                PRODUCER_THREADLOCAL = ThreadLocal.withInitial(() -> producer);
                INITIALIZE = true;
                Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
                log.info("生产者初始化完成");
            }
        }
    }

    /**
     * 构造函数初始化（测试使用要去掉）
     *
     * @param producerConfig 生成者配置
     */
    public MyKafkaProducer(KafkaProducerConfig producerConfig) {
        this.producerConfig = producerConfig;
        this.afterPropertiesSet();
    }

    /**
     * 服务关闭
     */
    private synchronized void shutdown() {
        INITIALIZE = false;
        if (null != PRODUCER_THREADLOCAL && null != PRODUCER_THREADLOCAL.get()) {
            PRODUCER_THREADLOCAL.get().close();
            PRODUCER_THREADLOCAL.remove();
        }
    }

    /**
     * 开启事务（如果代码块里面开启了异步线程那么事务不会生效）
     *
     * @param execute 需要执行的代码
     */
    public void openTransaction(TransactionExecute execute) {
        try {
            PRODUCER_THREADLOCAL.get().beginTransaction();
        } catch (Exception e) {
            System.out.println("开启事务失败:" + e.getMessage());
        }
        try {
            execute.doInTransaction();
            //提交事务
            PRODUCER_THREADLOCAL.get().commitTransaction();
        } catch (Exception e) {
            System.out.println("遇到异常事务回滚:" + e.getMessage());
            //回滚事务
            PRODUCER_THREADLOCAL.get().abortTransaction();
            throw e;
        }
    }


    /**
     * 同步发送（等待同步响应）
     * 同步发送，事务将会失效
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
            RecordMetadata metadata = PRODUCER_THREADLOCAL.get().send(record).get();
            System.out.println("发送成功，metadata：" + JSON.toJSONString(metadata));
            log.info("消息发送成功，topic:{},key:{},metadata{}", message.getTopic(), message.getKey(), metadata);
        } catch (Exception e) {
            log.error("生产者发送失败：{}", e);
            throw new KafkaException(kafkaExceptionEnum.PRODUCER_SEND_FAILURE.getValue(),
                    kafkaExceptionEnum.PRODUCER_SEND_FAILURE.getName() + e.getMessage());
        }
    }


    /**
     * 异步发送（无回调，发了就不管了）推荐使用
     *
     * @param message 消息内容
     * @param <T>     消息体泛型
     * @return Future<RecordMetadata>
     */
    public <T> Future<RecordMetadata> sendAsync(Message<T> message) {
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
            return PRODUCER_THREADLOCAL.get().send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("生产者发送失败：{}", exception);
                    throw new KafkaException(kafkaExceptionEnum.PRODUCER_SEND_FAILURE.getValue(),
                            kafkaExceptionEnum.PRODUCER_SEND_FAILURE.getName() + exception.getMessage());
                }
                log.info("消息发送成功,topic:{},key:{}", message.getTopic(), message.getKey());
            });
        } catch (Exception e) {
            log.error("生产者发送失败：{}", e);
            throw new KafkaException(kafkaExceptionEnum.PRODUCER_SEND_FAILURE.getValue(),
                    kafkaExceptionEnum.PRODUCER_SEND_FAILURE.getName() + e.getMessage());
        }
    }

    /**
     * 异步自定义回调发送
     *
     * @param message  消息内容
     * @param callback 回调内容
     * @param <T>      消息泛型
     * @return Future<RecordMetadata>
     */
    public <T> Future<RecordMetadata> sendAsync(Message<T> message, Callback callback) {
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
            return PRODUCER_THREADLOCAL.get().send(record, callback);
        } catch (Exception e) {
            log.error("生产者发送失败：{}", e);
            throw new KafkaException(kafkaExceptionEnum.PRODUCER_SEND_FAILURE.getValue(),
                    kafkaExceptionEnum.PRODUCER_SEND_FAILURE.getName() + e.getMessage());
        }
    }


    /**
     * 配置内容(使用protected)
     *
     * @return 配置内容
     */
    protected static Properties properties(KafkaProducerConfig producerConfig) {
        MyKafkaProducer.checkConfig(producerConfig);
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
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, producerConfig.isEnableIdempotence());
        //是否开启事物
        if (producerConfig.isEnableTransactional()) {
            //事务 ID (每台机器独立开启)
            properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerConfig.getAppName() + "_TRANSACTIONAL_ID_" + UUID.randomUUID().toString());
            //事务级别 (默认read_committed)
            properties.put("isolation.level", Optional.ofNullable(producerConfig.getIsolationLevel()).orElse("read_committed"));
        }
        //client ID
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, producerConfig.getAppName().toUpperCase() + "_CLIENT_ID_" + UUID.randomUUID().toString());
        return properties;
    }


    /**
     * 校验配置文件
     *
     * @param producerConfig 生成者配置
     */
    private static void checkConfig(KafkaProducerConfig producerConfig) {
        if (null == producerConfig) {
            throw new KafkaException(kafkaExceptionEnum.PRODUCER_CONFIGURATION_IS_EMPTY.getValue()
                    , kafkaExceptionEnum.PRODUCER_CONFIGURATION_IS_EMPTY.getName());
        }
        if (null == producerConfig.getAppName()) {
            throw new KafkaException(kafkaExceptionEnum.PRODUCER_APPNAME_IS_EMPTY.getValue()
                    , kafkaExceptionEnum.PRODUCER_APPNAME_IS_EMPTY.getName());
        }
    }


}
