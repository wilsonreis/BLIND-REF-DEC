package com.santander.kpv.services.sender;

import com.santander.kpv.exceptions.MyRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Value;

import javax.jms.*;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SendReceiverTextMessageService2Test {

    @Mock
    private JMSContext jmsContext;

    @Mock
    private Destination queueRequest;

    @Mock
    private Destination queueResponse;

    @Mock
    private TextMessage textMessage;

    @Mock
    private JMSProducer jmsProducer;

    @Mock
    private JMSConsumer jmsConsumer;

    @InjectMocks
    private SendReceiverTextMessageService sendReceiverTextMessageService;

    @Value("${ibm.mq.jmsExpiration}")
    private long jmsExpiration = 3600000; // 1 hour

    @BeforeEach
    void setUp() throws JMSException {
        sendReceiverTextMessageService.setJmsExpiration(jmsExpiration);
        when(jmsContext.createTextMessage(anyString())).thenReturn(textMessage);
        when(jmsContext.createProducer()).thenReturn(jmsProducer);
        when(jmsContext.createConsumer(any(Destination.class), anyString())).thenReturn(jmsConsumer);
    }

    @Test
    void testEnviaRecebeMensagens() throws JMSException {
        String mensagem = "testMessage";
        when(textMessage.getJMSCorrelationID()).thenReturn(UUID.randomUUID().toString());
        when(jmsConsumer.receiveBody(String.class, 15000)).thenReturn("responseMessage");

        String response = sendReceiverTextMessageService.enviaRecebeMensagens(mensagem);
        assertEquals("responseMessage", response);
    }

    @Test
    void testEnviaRecebeMensagensTimeout() throws JMSException {
        String mensagem = "testMessage";
        when(textMessage.getJMSCorrelationID()).thenReturn(UUID.randomUUID().toString());
        when(jmsConsumer.receiveBody(String.class, 15000)).thenReturn(null);

        Exception exception = assertThrows(MyRuntimeException.class, () -> {
            sendReceiverTextMessageService.enviaRecebeMensagens(mensagem);
        });
        assertEquals("Tempo de espera expirado", exception.getMessage());
    }
}
