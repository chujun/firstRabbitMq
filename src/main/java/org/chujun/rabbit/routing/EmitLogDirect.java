package org.chujun.rabbit.routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * Created by chujun on 16/5/5.
 * routingKey依赖于exchange type.
 * channel.queueBind(queueName, EXCHANGE_NAME, "black");
 * 对于fanout将忽视routingKey.灵活性比较差,它只能广播消息.
 * 而对于Direct exchange,会依据routingKey来路由.
 * 基于Direct exchange的算法是简单的:消息发送到binding key匹配消息routing key的队列.
 * (不匹配的消息将被丢弃)
 * 2.Multiple bindings
 * 这是合法的:同一个binding key绑定到多个队列中,
 */
public class EmitLogDirect {
    public static final String EXCHANGE_NAME="logs-severity";

    public static final String EXCHANGE_TYPE="direct";

    public static final String VIRTUAL_HOST="log";

    public static int count=1;

    public static String messageMain=":hello world ";
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory=new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setVirtualHost(VIRTUAL_HOST);
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE);
        //无需申明queue
        int messageCount=10000;
        for(int i=0;i<messageCount;i++){
            String message=getMessage(new Date()+messageMain+(count++));
            String severity = getSeverity(i);
            //routing key
            channel.basicPublish(EXCHANGE_NAME,severity,null,message.getBytes());
            System.out.println("send ["+severity+"]:'"+message+"'");
        }
        //channel.close();
        //connection.close();
    }

    public static String getSeverity(int random){
        String[] severitys=new String[]{"debug","info","warn","error"};
        return severitys[random%4];
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
