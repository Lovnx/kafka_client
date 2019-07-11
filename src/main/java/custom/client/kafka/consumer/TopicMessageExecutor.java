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
     *
     * @return topice名称
     */
    String getTopic();

    /**
     * 实例名称
     *
     * @return 当有多个实例的时候可以标记出唯一的实例
     */
    String getExecutorName();

    /**
     * 处理消息
     *
     * @param message 消息
     * @return 当使用自动提交的时候可以忽略，当使用手动提交的时候 true 消费成功 false消费失败
     */
    boolean execute(Message<T> message);

    /**
     * 获取消息类型
     *
     * @return 返回消息序列化类型
     */
    TypeReference<T> getTypeReference();

    /**
     * 是否需要手动提交(如果有多个消费同一个Topic只要有一个使用了手动提交全部都是手动提交)
     *
     * @return 是否需要手动提交
     */
    boolean needManualCommit();
}
