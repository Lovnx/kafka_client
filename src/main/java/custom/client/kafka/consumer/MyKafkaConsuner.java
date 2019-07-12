package custom.client.kafka.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import custom.client.kafka.Message.Message;
import custom.client.kafka.config.KafkaConsumerConfig;
import custom.client.kafka.exception.KafkaException;
import custom.client.kafka.exception.kafkaExceptionEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @program: kafka-test
 * @description: 自定义消费者实现
 * @author: ZengShiLin
 * @create: 2019-07-11 16:09
 **/
@Slf4j
public class MyKafkaConsuner implements InitializingBean {

    @Autowired(required = false)
    private KafkaConsumerConfig consumerConfig;

    /**
     * 注入所有实现 TopicMessageExecutor接口的bean
     */
    @Autowired(required = false)
    private List<TopicMessageExecutor> topicMessageExecutors = Lists.newArrayList();

    /**
     * 所有正在跑的进程
     */
    private List<ConsumerRunnable> consumerRunnables = Lists.newArrayList();

    /**
     * 使用线程池统一管理
     */
    private ThreadPoolExecutor ifinPoolExecutor;

    /**
     * 是否已经初始化（volatile 增强可见性）
     */
    private volatile static boolean INITIALIZE = false;

    /**
     * 当前服务的IP
     */
    private String ip;

    /**
     * 当前服务的HostName
     */
    private String hostName;

    @Override
    public void afterPropertiesSet() {
        InetAddress ia;
        try {
            //获取IP和HostName
            ia = InetAddress.getLocalHost();
            hostName = ia.getHostName();
            ip = ia.getHostAddress();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //TODO 读取消费者配置
        this.initConsume();
        //关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdow));
    }

    /**
     * 初始化消费者(同步)
     */
    private synchronized void initConsume() {
        if (INITIALIZE) {
            log.info("消费者已经初始化完成");
        }
        if (null == this.consumerConfig) {
            log.error("消费者配置为空");
            throw new KafkaException(kafkaExceptionEnum.CONSUMER_CONFIGURATION_IS_EMPTY.getValue()
                    , kafkaExceptionEnum.CONSUMER_CONFIGURATION_IS_EMPTY.getName());
        }
        //按照Topic分组 <Topic,topicMessageExecutors>
        Map<String, List<TopicMessageExecutor>> topicMessageExecutorsMap = topicMessageExecutors.stream()
                .collect(Collectors.groupingBy(TopicMessageExecutor::getTopic));
        if (CollectionUtils.isEmpty(topicMessageExecutorsMap)) {
            log.info("当前服务没有消费者实例");
            return;
        }

        //创建线程池(一个topic一个线程单独跑)
        ifinPoolExecutor = new ThreadPoolExecutor(
                topicMessageExecutorsMap.keySet().size(),
                //预留多一个线程
                topicMessageExecutorsMap.keySet().size() + 1,
                0L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(0),
                new BasicThreadFactory.Builder().namingPattern("kafka-consumer-thread-pool").build()
        );
        //遍历所有实现TopicMessageExecutor的 topice
        topicMessageExecutorsMap.forEach((key, value) -> {
            TopicMessageExecutor needManualCommit = value.stream().filter(TopicMessageExecutor::needManualCommit)
                    .findAny().orElse(null);
            Properties properties = this.getPropertie(consumerConfig, needManualCommit);
            //线程池运行
            ifinPoolExecutor.execute(new ConsumerRunnable(properties, value, 10));
        });

    }

    /**
     * 内部 Runnable（每个Runnable有一个KafkaConsumer）
     */
    private class ConsumerRunnable implements Runnable {

        /**
         * Kafka配置
         */
        private Properties properties;

        /**
         * 实现接口 TopicMessageExecutor 的实例
         */
        private List<TopicMessageExecutor> messageExecutors;

        /**
         * 当前Topic
         */
        private String topic;

        /**
         * 消费者是否运行中
         */
        private boolean consumering = true;

        /**
         * 是否正在消费（当消息数量多的时候如果需要下线服务，需要等待消息消费完成）
         */
        private boolean isRun;

        /**
         * 当前线程的KafkaConsumer
         * KafkaConsumer不是线程安全的
         */
        private KafkaConsumer<String, String> consumer;

        /**
         * 自动提交阈值
         */
        private Integer autoCommitSize;

        /**
         * 构造函数
         *
         * @param properties       消费者配置
         * @param messageExecutors 实现接口 TopicMessageExecutor 的实例
         */
        ConsumerRunnable(Properties properties, List<TopicMessageExecutor> messageExecutors, Integer autoCommitSize) {
            this.properties = properties;
            this.topic = messageExecutors.get(0).getTopic();
            this.messageExecutors = messageExecutors;
            this.autoCommitSize = autoCommitSize;
        }

        @Override
        public void run() {
            this.consumer = new KafkaConsumer<>(this.properties);
            //手动提交使用的Map
            Map<TopicPartition, OffsetAndMetadata> metadataMap = Maps.newHashMap();
            while (consumering) {
                //100ms 间隔主动抓取一次
                ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));
                int count = 0;
                isRun = true;
                for (ConsumerRecord<String, String> record : records) {
                    log.info("topic:" + record.topic() + ",partition=" + records.partitions() + ", offset = " + record.offset() + ", key = " + record.key() + ", value = " + record.value());
                    TypeReference typeReference = this.messageExecutors.get(0).getTypeReference();
                    //消息序列化成指定的DTO格式（传输数据都需要使用DTO封装起来）
                    Object obj = JSON.parseObject(record.value(), typeReference.getType());
                    Message message = new Message<>(record.key(), record.topic(), obj);
                    boolean success = false;
                    //广播消费消息（如果有多个实例相当于广播,Message使用了final不用担心被其它实例修改）,并且实例和实例之间不能互相影响 TODO 这里消费成功的方式有待考量，不过目前都是单实例的问题不大
                    for (TopicMessageExecutor temp : this.messageExecutors) {
                        try {
                            success = temp.execute(message);
                        } catch (Exception e) {
                            log.error("kafka消费失败,实例名称:{},Topic,{},Exception：{}", temp.getExecutorName(), message.getTopic(), e);
                        }
                    }
                    //当前消息消费成功
                    if (success) {
                        //消费信息：【消费系统 + 消息Key + 消费时间毫秒 + 系统IP + 系统HostName】
                        String info = consumerConfig.getAppName() + "_" + record.key() + "_" + System.currentTimeMillis() + "_" + ip + "_" + hostName;
                        //手动提交map  offset + 1，下次消费者从该偏移量开始拉取消息 (metadata 提交的一些额外信息)
                        metadataMap.put(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1, info));
                    }
                    //判断是否需要自动提交(这个Key存的就是boolean)
                    if (!(boolean) properties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
                        count++;
                    }
                    //达到提交的阈值（有时候一批数据特别多，这个时候就需要批量提交了 默认10提交一次，可以自行设置）
                    if (autoCommitSize <= count) {
                        //手动提交 同步
                        this.consumer.commitSync(metadataMap);
                        count = 0;
                        //清空commit信息
                        metadataMap.clear();
                    }
                }
                if (!(boolean) properties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
                    //手动提交 同步
                    this.consumer.commitSync(metadataMap);
                    //清空commit信息
                    metadataMap.clear();
                }
                isRun = false;
            }
        }
    }


    /**
     * 获取消费者配置
     *
     * @param consumerConfig 消费者配置
     * @return 消费者配置
     */
    private Properties getPropertie(KafkaConsumerConfig consumerConfig, TopicMessageExecutor needManualCommit) {
        Properties properties = new Properties();
        //涉及的broker
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerConfig.getBootstrapServers());
        //健反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //值反序列化器
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        if (null != needManualCommit) {
            //当前Topic需要手动提交
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        }
        //消费者会自动Rebalance
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerConfig.getAppName());
        properties.put("auto.offset.reset", "latest");
        //下面这两个值是影响 消费者 Rebalance的
        //2秒提交一次心跳
        properties.put("auto.commit.interval.ms", "2000");
        //7秒没收到心跳就剔除消费者组进行Rebalance
        properties.put("session.timeout.ms", "7000");
        //一次拉取消息大小,当设置成1的时候，几乎就算一个一个消息消费了（如果单个消息大于这个值，就返回单条消息）（默认值52428800 大概是 50MB）
        //这个配置能有效提升吞吐量
        properties.put("max.partition.fetch.bytes", 52428800);
        properties.put("fetch.max.bytes", 52428800);
        return properties;
    }


    /**
     * 关闭消费者
     */
    private synchronized void shutdow() {
        consumerRunnables.forEach(e -> {
            while (e.isRun) {
                //自旋等待消息消费完成
            }
            e.consumering = false;
            e.consumer.close();
            log.info("消费者，topice:{},已经关闭", e.topic);
        });
        INITIALIZE = false;
        log.info("系统kafka消费者们已经关闭");
    }

}
