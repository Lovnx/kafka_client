package custom.client.kafka.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.springframework.stereotype.Component;

/**
 * 参考官网配置:http://kafka.apache.org/documentation/
 * Create by ZengShiLin on 2019-07-07
 * 生产者配置
 * TODO 后续是否考虑拓展更多方案?比如 ssl加密支持
 *
 * @author ZengShiLin
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
@Component
public class ProducerConfig {

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
     * Kafka消息健序列化器(如果使用作为机器学习或者其它方面监控,那么会对图像或者视频进行序列化)
     */
    private String keySerializer;

    /**
     * kafka消息值序列化器(如果使用作为机器学习或者其它方面监控,那么会对图像或者视频进行序列化)
     */
    private String valueSerializer;


    /**
     * 是否开启幂等性 true 是 false 否
     * kafka 提供三种等级的消息交付一致性
     * 最多一次（at most once）：消息可能会丢失,但绝不不会被重复发送
     * 至少一次 (at least once): 消息不会丢失,但是可能被重复发送
     * 精确一次 (exactly once): 消息不会丢失,也不会被重复发送
     * 开启幂等性是在broker保证精确一致性(前提是kafka消息的key值必须全局唯一)
     */
    private String enableIdempotence;

    /**
     * 项目名称
     * repair-web finance-service 等等
     */
    private String appName;


    /**
     * 是否开启事务模式
     * true 是 false 否
     */
    private boolean enableTransactional;


    /**
     * 使用的压缩算法
     * 默认不开启
     * 支持:gzip,snappy(facebook的高压缩率算法),lz4,zstd
     */
    private String compressionType;

    /**
     * 重试次数
     */
    private String retries;

    /**
     * 发送超时时间（默认120秒）
     */
    private String deliveryTimeoutMs;


}
