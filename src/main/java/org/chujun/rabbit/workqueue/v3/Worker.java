package org.chujun.rabbit.workqueue.v3;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * Created by chujun on 16/4/1.
 * 消息持久化机制(Message durability)
 * 之前我们学习了:即使消费者突然死亡,消息任务也不会丢失.但是当RabbitMq服务器停止时,消息仍然会丢失.
 * 当RabbitMq停止或者奔溃时,它将丢失队列和消息,除非你告诉它不这么做(持久化).
 * 确保消息不会丢失需要做两件事情:我们需要同时确保队列和消息都是持久化的.
 * 首先,我们需要确保RabbitMq不会丢失我们的队列.为了这么做我们需要确保队列是持久化的.
 *
 *
 * 关于消息持久化说明
 * 标记消息为持久化的并不能完全保证消息不会丢失.尽管他告诉RabbitMq保存消息到硬盘中去,但这里仍有一个小的时间窗口:
 * 当RabbitMq已经接受了消息,然而还没有保存它.
 * 另外,RabbitMq不会为所有消息调用fsync(2)--这导致消息可能仅仅只是存储到缓存中去而没有真正写到硬盘中去.
 * 持久化保证不是强有力的,但是对我们简单的任务队列是足够的了.如果你需要更强保证,你可以使用发布者确认机制(publisher confirms).
 */
public class Worker {
    public static final String QUEUE_NAME="durable work queues";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory=new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();
        //使队列持久化
        boolean durable=true;

        channel.queueDeclare(QUEUE_NAME,durable,false,false,null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        channel.basicQos(1);


        Consumer consumer=new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag,Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException{
                String message=new String(body,"UTF-8");
                System.out.println("[x] Received '"+message+"'");
                try {
                    doWork(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }finally {
                    System.out.println("[x]"+new Date()+": done '"+message+"'\n");
                    channel.basicAck(envelope.getDeliveryTag(),false);
                }
            }
        };
        channel.basicConsume(QUEUE_NAME,false,consumer);
    }

    /**
     * 通过解析消息中的"."模拟处理消息的耗时秒数
     * @param message
     */
    public static void doWork(String message) throws InterruptedException {
        for(char ch:message.toCharArray()){
            if(ch=='.'){
                Thread.sleep(1000);
            }
        }
    }
}
