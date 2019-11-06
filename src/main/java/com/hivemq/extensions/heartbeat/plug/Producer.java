package com.hivemq.extensions.heartbeat.plug;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: heaven
 * @Date: 2019/8/19 10:29
 * @Version 1.0
 */
public class Producer {

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    static RPCHook getAclRPCHook() {
        return new AclClientRPCHook(new SessionCredentials("rocketmq2","aiot@2019!Q@W#E!@"));
    }

    public static void send(String topic, String msg) throws MQClientException, InterruptedException {

        //需要一个producer group名字作为构造方法的参数，这里为producer1
        DefaultMQProducer producer2 = new DefaultMQProducer("producer2",getAclRPCHook());

        //设置NameServer地址,此处应改为实际NameServer地址，多个地址之间用；分隔
        //NameServer的地址必须有，但是也可以通过环境变量的方式设置，不一定非得写死在代码里
        producer2.setNamesrvAddr("110.43.34.130:9876;110.43.34.135:9876");
        producer2.setVipChannelEnabled(false);

        //为避免程序启动的时候报错，添加此代码，可以让rocketMq自动创建topickey
//        producer.setCreateTopicKey("AUTO_CREATE_TOPIC_KEY");
        producer2.start();

            try {
                Message message = new Message(topic, "*",
                        msg.getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer2.send(message);
                log.info("发送的消息topic:{},msg:{}",topic,msg);
                log.info("发送的消息ID:" + sendResult.getMsgId() +"--- 发送消息的状态：" + sendResult.getSendStatus());
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        producer2.shutdown();

    }

    public static void main(String[] args) {
        try {
//            for(int i=0;i<10;i++) {
                send("topicB", "xxxxxxxxx");
//            }
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
