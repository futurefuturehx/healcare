import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import java.util.List;

public class Consumer {
    public static void main(String[] args) throws MQClientException {
        //创建消费者
        DefaultMQPushConsumer consumer=new DefaultMQPushConsumer("rmq-group");
        //设置NameServer地址
        consumer.setNamesrvAddr("127.0.0.1:9876");
        //设置实例名称
        consumer.setInstanceName("consumer");
        //订阅topic
        consumer.subscribe("itmayiedu-topic","TagA");

        //监听消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                //获取消息
                for (MessageExt messageExt:list){
                    //RocketMQ由于是集群环境，所有产生的消息ID可能会重复
                    System.out.println(messageExt.getMsgId()+"---"+new String(messageExt.getBody()));
                }
                //接受消息状态 1.消费成功    2.消费失败   队列还有
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动消费者
        consumer.start();
        System.out.println("consumer Started!");
    }
}