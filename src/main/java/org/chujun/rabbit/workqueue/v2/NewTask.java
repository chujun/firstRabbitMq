package org.chujun.rabbit.workqueue.v2;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * Created by chujun on 16/3/31.
 * one sender,many consumers
 */
public class NewTask {
    public static final String QUEUE_NAME="work queues";

    public static int count=1;

    public static String messageMain=":hello world ";
    public static void main(String[] args) throws IOException, TimeoutException {
        System.out.println("start message acknowledgment");
        ConnectionFactory connectionFactory=new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME,false,false,false,null);
        int messageCount=10;
        for(int i=0;i<messageCount;i++){
            String message=getMessage(new Date()+messageMain+(count++));
            channel.basicPublish("",QUEUE_NAME,null,message.getBytes());
            System.out.println("send :'"+message+"'");
        }
    }

    public static String getMessage(String message){
        String default_message=messageMain;
        if(null==message){
            message=new Date()+default_message+(count++);
        }
        int count=generateRandomCount(10);
        for(int i=0;i<count;i++){
            message+=".";
        }
        return message;
    }

    /**
     *产生1到baseNum的数
     * @param baseNum
     * @return
     */
    public static int generateRandomCount(Integer baseNum){
        int num=10;
        if(null!=baseNum&&baseNum>0){
            num=baseNum;
        }
        int count=(int)(1+Math.random()*baseNum);
        System.out.println("current count:"+count);
        return count;
    }
}
