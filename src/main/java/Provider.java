import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

/**
 * 生产者
 */
public class Provider {
    public static void main(String[] args) throws MQClientException {
        //创建一个生产者
        DefaultMQProducer producer=new DefaultMQProducer("rmq-group");
        //设置NameServer地址
        producer.setNamesrvAddr("127.0.0.1:9876");
        //设置生产者实例名称
        producer.setInstanceName("producer");
        //启动生产者
        producer.start();

        try {
            //发送消息
            for (int i=1;i<=10;i++){
                //模拟网络延迟，每秒发送一次MQ
                Thread.sleep(1000);
                //创建消息，topic主题名称  tags临时值代表小分类， body代表消息体
                Message message=new Message("itmayiedu-topic","TagA",("itmayiedu-"+i).getBytes());
                //发送消息
                SendResult sendResult=producer.send(message);
                System.out.println("来了来了："+sendResult.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.shutdown();
    }
}