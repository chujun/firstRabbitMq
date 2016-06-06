package org.chujun.rabbit.topics;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * Created by chujun on 16/5/5.
 * https://www.rabbitmq.com/tutorials/tutorial-five-java.html
 *
 * 使用 direct exchange可以提高系统灵活性,但仍有限制-它不能基于多标准路由。
 * 在我们的日志系统中我们想订阅的不仅仅是基于严重性的日志,也基于产生日志的来源。(syslog unix util)
 * 为此我们需要学习更灵活的topic exchange。
 *
 *
 * 发送到topic exchange的消息不能有随意的routing key。它必须是一系列以(.)分隔的单词。单词可以是任意的,
 * 但通常都说明了连接消息的某些特征。一些合法的routing key例如:
 *  "stock.usd.nyse", "nyse.vmw", "quick.orange.rabbit"
 *  routing key的字符数有有上限:255.
 *
 *  队列的binding key也必须保持相同的格式。
 *  topic exchange的routing规则与direct规则类似。带有特定routing key的消息将会被分发到所有匹配binding key的队列中去。
 *  对于binding key有两点特殊说明:
 *  1.*(star)可以替代一个词。
 *  2.#(hash)可以替代0或多个词。
 *
 *
 *
 *  We created three bindings:
 *  Q1 is bound with binding key "*.orange.*"
 *  Q2 with "*.*.rabbit" and "lazy.#".
 *
 *  "quick.orange.rabbit"->Q1,Q2
 *  "lazy.orange.elephant"->Q1,Q2
 *
 *  "quick.orange.fox" ->Q1
 *  "lazy.brown.fox"->Q2
 *
 *
 *  "lazy.pink.rabbit"->Q2(这里需要注意的是:该消息只会分发到队列2一次,尽管它匹配两个binding key)
 *
 *   "quick.brown.fox"->none,会被遗弃。
 *
 *   "lazy.orange.male.rabbit"->Q2
 *
 *   topic exchange与其他exchange的关系
 *   1. When a queue is bound with "#" (hash) binding key -
 *   it will receive all the messages, regardless of the routing key - like in fanout exchange.
 *   "#"
 *   2.When special characters "*" (star) and "#" (hash) aren't used in bindings,
 *   the topic exchange will behave just like a direct one.
 *
 *
 */
public class EmitLogTopic {
    public static final String EXCHANGE_NAME="logs-severity-source";

    public static final String EXCHANGE_TYPE="topic";

    public static final String VIRTUAL_HOST="log-topic";

    private static final String USER_NAME = "study";

    private static final String PASSWORD = "study";

    public static final String[] SOURCES=new String[]{"kern","normal","program"};

    public static int count=1;

    public static String messageMain=":hello world ";
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory=new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setVirtualHost(VIRTUAL_HOST);
        connectionFactory.setUsername(USER_NAME);
        connectionFactory.setPassword(PASSWORD);
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE);
        //无需申明queue
        int messageCount=10000;
        for(int i=0;i<messageCount;i++){
            String source=getSource(i);
            String severity = getSeverity(i);
            String message=getMessage(new Date()+"["+source+"]["+severity+"]"+messageMain+(count++));
            //routing key
            channel.basicPublish(EXCHANGE_NAME,source+"."+severity,null,message.getBytes());
            System.out.println("send["+source+"]["+severity+"]:'"+message+"'");
        }
        //channel.close();
        //connection.close();
    }

    public static String getSource(int random){
        return SOURCES[random%SOURCES.length];
    }

    public static String getSeverity(int random){
        String[] severitys=new String[]{"debug","info","warn","error"};
        return severitys[random%severitys.length];
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
