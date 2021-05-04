package top.chenfu.product;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.CountDownLatch2;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.Arrays;
import java.util.List;

public class TransactionProducer {

    public static void main(String[] args) throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer();
        producer.setProducerGroup("transactionProducerGroup");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        final CountDownLatch2 countDownLatch2 = new CountDownLatch2(10);
        producer.setTransactionListener(new TransactionListener() {
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                System.out.println("executeLocalTransaction" + msg + arg);
                if (StringUtils.equals("TagA", msg.getTags())) {
                    countDownLatch2.countDown();
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else if (StringUtils.equals("TagB", msg.getTags())) {
                    return LocalTransactionState.UNKNOW;
                } else {
                    countDownLatch2.countDown();
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
            }

            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                System.out.println("checkLocalTransaction" + msg);
                countDownLatch2.countDown();
                return LocalTransactionState.COMMIT_MESSAGE;
            }

        });
        List<String> tagList = Arrays.asList("TagA", "TagB", "TagC");
        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            message.setTopic("transactionTopic");
            message.setTags(tagList.get(i % tagList.size()));
            message.setBody(("trasaction Message " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            message.setTransactionId("tx-" + i);
            producer.sendMessageInTransaction(message, i);
        }
        countDownLatch2.await();
        producer.shutdown();
    }

}
