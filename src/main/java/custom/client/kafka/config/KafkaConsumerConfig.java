package custom.client.kafka.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * 参考官网配置:http://kafka.apache.org/documentation/
 * Create by ZengShiLin on 2019-07-07
 *
 * @author ZengShiLin
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class KafkaConsumerConfig {

    /**
     * kafka borkers 集群,以前是zookeeper集群(从2.x版本开始kafka转变配置为broker机器IP)
     * 如果集群超过三台机器,只要配置你使用topice涉及的即可
     */
    private String bootstrapServers;

    /**
     * broker收到消息回复响应级别
     * 值: [all, -1, 0, 1]
     * all:等待leader等待所有broker同步完副本回复 [all 等价于 -1]
     * 1 :leader同步完直接回复,不等待所有broker同步
     * 0 :leader不等待同步,直接响应
     * 在内网环境下,目前没有任何事实证明 all级别 TPS 就一定很差或者远远小于 0级别
     * [在没有性能问题之前我建议大家都使用all因为这样比较安全]
     */
    private String acks;

    /**
     * 健反序列化器(如果使用作为机器学习或者其它方面监控,那么会对图像或者视频进行序列化)
     */
    private String keyDeserializer;

    /**
     * 值反序列化器(如果使用作为机器学习或者其它方面监控,那么会对图像或者视频进行序列化)
     */
    private String valueDeserializer;

    /**
     * 是否自动提交(如果生成者开启了事务,我建议关闭)
     * 是:true  否:false
     */
    private String enableAutoCommit;

    /**
     * 这是一个比较坑的配置 自动重置 offset的方式 [没有合理配置会导致消费者消费不到消息]
     * 当Kafka中没有初始偏移量，或者当前偏移量在服务器上不存在时(例如，因为数据已被删除)，该怎么办?
     * earliest:使用最早的偏移量
     * latest:使用最新的offset[默认,并建议使用]
     * none:抛出异常
     * anything:随机选取一个
     */
    private String autoOffsetReset;

    /**
     * 消费者心跳时间（默认10秒）
     */
    private String sessionTimeoutMs;

    /**
     * 如果开启了自动提交
     * 消费者自动提交到kafka的频率（默认5秒）
     */
    private String autoCommitIntervalMs;

}
