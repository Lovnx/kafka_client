package custom.client.kafka.monitor;

import com.google.common.collect.Maps;
import custom.client.kafka.config.KafkaConsumerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @program: my-kafka-client
 * @description: kafka消费者监控
 * 为什么要监控消费者？
 * 1、如果消息堆积过多会导致消费缓慢，也可能是消息丢失
 * 2、当消息滞后到一定程度的时候，kafka的零拷贝技术会失去效果
 * 3、消息积压及时告警有助于增加partition和consumer去提升吞吐量
 * @author: ZengShiLin
 * @create: 2019-07-24 09:38
 **/
@Slf4j
public class Monitor implements InitializingBean {

    @Autowired
    private KafkaConsumerConfig consumerConfig;

    /**
     * 告警阈值
     */
    private static Long ALARM_THRESHOLD = 10L;

    /**
     * 用于周期性监控线程池的运行状态
     */
    private static ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new BasicThreadFactory.Builder().namingPattern("thread-pool-monitor").build());

    /**
     * 定时监控
     */
    @Override
    public void afterPropertiesSet() {
        //消费者监控一分钟输出一次监控
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            Map<TopicPartition, Long> topicPartitionLongMap = this.lagOf(consumerConfig.getAppName().toUpperCase(), consumerConfig.getBootstrapServers());
            if (CollectionUtils.isEmpty(topicPartitionLongMap)) {
                log.info("kafka消费者本次监控信息为空");
            }
            topicPartitionLongMap.forEach((key, value) -> {
                if (value > ALARM_THRESHOLD) {
                    log.error("发生消息滞后,Topic:{},Partition:{},消息差距：{}", key.topic(), key.partition(), value);
                } else {
                    log.info("Topic:{},Partition:{},消息差距：{}", key.topic(), key.partition(), value);
                }
            });
            //一分钟一次监控结果
        }, 0, 1, TimeUnit.MINUTES);
    }

    /**
     * 获取消费者组的 lag
     *
     * @param groupID         消费者组ID
     * @param bootstrapServer broker集群IP
     * @return key<TopicPartition, 消息offset差距>
     */
    private Map<TopicPartition, Long> lagOf(String groupID, String bootstrapServer) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        try (AdminClient client = AdminClient.create(props)) {
            ListConsumerGroupOffsetsResult result = client.listConsumerGroupOffsets(groupID);
            try {
                //设置超时时间
                Map<TopicPartition, OffsetAndMetadata> consumedOffsets = result.partitionsToOffsetAndMetadata().get(1, TimeUnit.MINUTES);
                //禁止自动提交
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
                //找到Topic
                props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
                //健反序列化器
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                //值反序列化器
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                    Map<TopicPartition, Long> endOffsers = consumer.endOffsets(consumedOffsets.keySet());
                    return endOffsers.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                            //当前consumer组的HW  - 当前消费者组的offset = 消息消费差距
                            entry -> entry.getValue() - consumedOffsets.get(entry.getKey()).offset()));
                } catch (Exception e) {
                    log.error("监控创建KafkaConsumer失败");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                //中断异常 Future 抛出
                log.error("监控出现InterruptedException异常");
            } catch (ExecutionException e) {
                // 处理 ExecutionException 异常 Future 抛出
                log.error("获取consumedOffsets抛出ExecutionException");
            } catch (TimeoutException e) {
                // 处理超时异常 Future 抛出
                log.error("获取consumedOffsets,超时");
            }
            return Maps.newHashMap();
        } catch (Exception e) {
            //其它异常
            return Maps.newHashMap();
        }
    }

}
