package custom.client.kafka.consumer;

import com.alibaba.fastjson.TypeReference;
import custom.client.kafka.Message.Message;

/**
 * @program: kafka-test
 * @description: topice对应的实现，当消费者收集到消息返回给这些实例
 * @author: ZengShiLin
 * @create: 2019-07-11 16:12
 **/
public interface TopicMessageExecutor<T> {

    /**
     * 获取Topic名称
     * TODO 后续允许做成一个实例消费多个Topic
     *
     * @return topice名称
     */
    String getTopic();

    /**
     * 实例名称(可以不填，只是为了方便排查线上问题)
     *
     * @return 当有多个实例的时候可以标记出唯一的实例
     */
    String getUniqueName();

    /**
     * 处理消息
     *
     * @param message 消息
     * @return 当使用自动提交的时候可以忽略，当使用手动提交的时候 true 消费成功 false消费失败
     */
    boolean execute(Message<T> message);

    /**
     * 获取消息类型(由于使用的是Fastjson TypeReference 更加安全)
     * 如果不给类型,直接返回String-json
     *
     * @return 返回消息序列化类型
     */
    TypeReference<T> getTypeReference();

    /**
     * 是否需要手动提交(如果有多个消费同一个Topic只要有一个使用了手动提交全部都是手动提交)
     * 说明：【自动提交】是poll的时候提交上一批拉取到的数据的offset，假设一次拉取到1000条数据，那么1000在第二次拉取才提交
     * 【手动提交】是按照设置，定期提交offset
     *
     * @return 是否需要手动提交
     */
    boolean needManualCommit();


    /**
     * 提交阈值（自定义）
     * 假设是1，那么消费一条消息就提交一次
     * 假设如果是2，那么消费两条消息后一起提交
     * 如果是0，消费完当前批次才提交
     *
     * @return 阈值
     */
    int commitThreshold();

}
