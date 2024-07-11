package com.santander.kpv.config;

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Queue;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MQTextMessageConfigurationTest {

    private MQTextMessageConfiguration configuration;

    @BeforeEach
    void setUp() {
        configuration = new MQTextMessageConfiguration();

        // Set properties using reflection
        ReflectionTestUtils.setField(configuration, "mqHostName", "localhost");
        ReflectionTestUtils.setField(configuration, "mqPort", 1414);
        ReflectionTestUtils.setField(configuration, "mqQueueManager", "QM1");
        ReflectionTestUtils.setField(configuration, "mqChannel", "DEV.APP.SVRCONN");
        ReflectionTestUtils.setField(configuration, "user", "app");
        ReflectionTestUtils.setField(configuration, "password", "passw0rd");
        ReflectionTestUtils.setField(configuration, "queueRequest", "REQUEST.QUEUE");
        ReflectionTestUtils.setField(configuration, "queueResponse", "RESPONSE.QUEUE");
    }

    @Test
    void testJmsConnectionFactory() throws JMSException {
        JmsConnectionFactory factory = configuration.jmsConnectionFactory();

        assertNotNull(factory);
        assertEquals("localhost", factory.getStringProperty(WMQConstants.WMQ_HOST_NAME));
        assertEquals(1414, factory.getIntProperty(WMQConstants.WMQ_PORT));
        assertEquals("DEV.APP.SVRCONN", factory.getStringProperty(WMQConstants.WMQ_CHANNEL));
        assertEquals(WMQConstants.WMQ_CM_CLIENT, factory.getIntProperty(WMQConstants.WMQ_CONNECTION_MODE));
        assertEquals("QM1", factory.getStringProperty(WMQConstants.WMQ_QUEUE_MANAGER));
        assertEquals("JmsPutGet (JMS)", factory.getStringProperty(WMQConstants.WMQ_APPLICATIONNAME));
        assertTrue(factory.getBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP));
        assertEquals("app", factory.getStringProperty(WMQConstants.USERID));
        assertEquals("passw0rd", factory.getStringProperty(WMQConstants.PASSWORD));
    }

    @Test
    void testJmsContext() {
        JmsConnectionFactory factory = Mockito.mock(JmsConnectionFactory.class);
        JMSContext context = Mockito.mock(JMSContext.class);

        when(factory.createContext()).thenReturn(context);

        JMSContext jmsContext = configuration.jmsContext(factory);

        assertNotNull(jmsContext);
        assertEquals(context, jmsContext);
    }

    @Test
    void testQueueRequest() {
        JMSContext context = Mockito.mock(JMSContext.class);
        Queue queue = Mockito.mock(Queue.class);

        when(context.createQueue("REQUEST.QUEUE")).thenReturn(queue);

        Destination queueRequest = configuration.queueRequest(context);

        assertNotNull(queueRequest);
        assertEquals(queue, queueRequest);
    }

    @Test
    void testQueueResponse() {
        JMSContext context = Mockito.mock(JMSContext.class);
        Queue queue = Mockito.mock(Queue.class);

        when(context.createQueue("RESPONSE.QUEUE")).thenReturn(queue);

        Destination queueResponse = configuration.queueResponse(context);

        assertNotNull(queueResponse);
        assertEquals(queue, queueResponse);
    }
}
