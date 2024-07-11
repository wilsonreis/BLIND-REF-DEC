package com.santander.kpv.config;


import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import com.santander.kpv.exceptions.MyRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.EnableJms;

import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSException;

@Configuration
@EnableJms
@Slf4j
public class MQTextMessageDecoupledConfiguration {

    @Value("${ibm.mq.host}")
    private String mqHostName;

    @Value("${ibm.mq.port}")
    private int mqPort;

    @Value("${ibm.mq.queueManager}")
    private String mqQueueManager;

    @Value("${ibm.mq.channel}")
    private String mqChannel;

    @Value("${ibm.mq.user}")
    private String user;
    @Value("${ibm.mq.password}")
    private String password;
    @Value("${ibm.mq.queueRequest}")
    private String queueRequest;

    @Value("${ibm.mq.queueResponse}")
    private String queueResponse;

    public String getQueueRequest() {
        return queueRequest;
    }

    public String getQueueResponse() {
        return queueResponse;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getMqHostName() {
        return mqHostName;
    }

    public int getMqPort() {
        return mqPort;
    }

    public String getMqQueueManager() {
        return mqQueueManager;
    }

    public String getMqChannel() {
        return mqChannel;
    }

    @Bean("jmsConnectionDecoupledFactory")
    public JmsConnectionFactory jmsConnectionFactory() {
        try {
            JmsFactoryFactory ff = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);
            JmsConnectionFactory cf = ff.createConnectionFactory();
            cf.setStringProperty(WMQConstants.WMQ_HOST_NAME, getMqHostName());
            cf.setIntProperty(WMQConstants.WMQ_PORT, getMqPort());
            cf.setStringProperty(WMQConstants.WMQ_CHANNEL, getMqChannel());
            cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_BINDINGS_THEN_CLIENT);
            cf.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, getMqQueueManager());
            cf.setStringProperty(WMQConstants.USERID, "admin");
            cf.setStringProperty(WMQConstants.PASSWORD, "passw0rd");
            return cf;
        } catch (Exception e) {
            log.error("JmsConnectionFactory jmsConnectionFactory() {} ", e.getMessage());
            throw new MyRuntimeException("Error creating MQ connection factory", e);
        }
    }
}