package top.chenfu.product;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;

public class AsyncProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setProducerGroup("asyncProducerGroup");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        producer.setRetryTimesWhenSendAsyncFailed(0);
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        System.out.println(countDownLatch.getCount());
        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            message.setTopic("asyncTopic");
            message.setTags("tagB");
            message.setKeys("aaa");
            message.setBody(("AsyncProducer - message " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            final int finalI = i;
            producer.send(message, new SendCallback() {
                public void onSuccess(SendResult sendResult) {
                    countDownLatch.countDown();
                    System.out.printf("%-10d OK %s %n", finalI, sendResult.getMsgId());
                }

                public void onException(Throwable e) {
                    countDownLatch.countDown();
                    System.out.printf("%-10d Exception %s %n", finalI, e.getMessage());
                }
            });
        }
        countDownLatch.await();
        producer.shutdown();
    }
}
