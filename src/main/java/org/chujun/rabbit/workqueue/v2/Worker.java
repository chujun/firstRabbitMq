package org.chujun.rabbit.workqueue.v2;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * Created by chujun on 16/4/1.
 * 消息持久化问题
 * reference:
 * https://www.rabbitmq.com/tutorials/tutorial-two-java.html
 * 1.问题描述:
 * 处理一个任务可能需要几秒钟.你可能会想知道:当消费者处理一个耗时任务,只处理了部分消费者就"死亡"了,此时会发生什么情况.
 * 在当前代码中(v1),一旦RabbitMq服务器讲消息发送给消费者,它将立刻从内存中删除该消息.
 * 这种情况下,你若杀死消费者进程,将会丢失消费者正在处理中的消息.我们将失去所有分发给该消费者的但还没有处理完的消息.
 *
 * 2.冲突,需求:
 * 很显然,我们不想失去任何消息任务.我们希望当消费者工作死亡时,能够将消息任务分发给另一个消费者.
 * 为了确保消息不会丢失,RabbitMq支持消息应答机制.
 *  消费者会发送一个ack(应答)给RabbitMq说明特定的消息已经被接收,处理.RabbitMq可以自由删除消息.
 *  如果消费者死亡(比如channel关闭,connection关闭,或者TCP链接关闭)没有发送一个应答,RabbitMq将认为该消息没有被完全处理,将会重新放到队列中(re-queue).
 *  如果同时存在另一个消费者,RabbitMq将很快重新发送该消息给另一个消费者.这种方式你可以确保消息没有丢失,即使消费者突然死亡.
 *
 *
 *  不存在消息超时(没看懂),RabbitMq会重新发送消息当消费者死亡.即使处理消息会花费一个很长的时间也是没问题的.
 *  消息应答默认开启.在前面一个例子中(v1)我们显示关闭了应答机制通过autoAck=true标志.
 *  是时候移除这个标志了,从消息工作者那里发送一个合适的应答,一旦我们处理完任务.
 * 3.解决方案:
 * 消息应答机制
 * In order to make sure a message is never lost, RabbitMQ supports message acknowledgments
 */
public class Worker {
    public static final String QUEUE_NAME="work queues";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory=new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME,false,false,false,null);
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
        channel.basicConsume(QUEUE_NAME,true,consumer);
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
