package top.chenfu.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class SimpleConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
        consumer.setConsumerGroup("chenConsumer");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.subscribe("fu", "*");
        consumer.subscribe("asyncTopic", "*");
        consumer.subscribe("oneWayTopic", "*");
        consumer.subscribe("scheduleTopic", "*");
        consumer.subscribe("batchTopic", "TagA");
        consumer.subscribe("transactionTopic", "*");
//        消费模式
//        负载均衡|集群【消费者采用负载均衡方式消费消息，多个消费者共同消费队列消息，每个消费者处理的消息不同】
        consumer.setMessageModel(MessageModel.CLUSTERING);
//        广播模式【消费者采用广播的方式消费消息，每个消费者消费的消息都是相同的】
//        consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("%s RECEIVE %s %n", Thread.currentThread().getName(), msg);
                    System.out.println(new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }

}
