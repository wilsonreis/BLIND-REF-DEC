package com.santander.kpv.services.sender;

import com.santander.kpv.exceptions.MyRuntimeException;
import com.santander.kpv.utils.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.jms.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SendReceiverTextMessageServiceTest {

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

    @BeforeEach
    void setUp() {
        sendReceiverTextMessageService.setJmsExpiration(3600000); // 1 hour
        textMessage = jmsContext.createTextMessage("mensagem");
    }

    @Test
    void testEnviaRecebeMensagensSFHInvalidMessage() {
        String cpf = "12345678901";
        String sfh = "msg";

        String response = sendReceiverTextMessageService.enviaRecebeMensagensSFH(cpf, sfh);
        assertEquals("Erro de parâmetro SFH", response);
    }


}