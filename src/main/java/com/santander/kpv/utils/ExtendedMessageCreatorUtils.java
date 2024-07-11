package com.santander.kpv.utils;

import jakarta.jms.*;
import org.springframework.jms.core.MessageCreator;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class ExtendedMessageCreatorUtils<T extends Message> implements MessageCreator {
    private Message message;
    private Class<T> messageType;

    public ExtendedMessageCreatorUtils() {
        Type genericSuperclass = this.getClass().getGenericSuperclass();
        ParameterizedType type = (ParameterizedType) genericSuperclass;
        this.messageType = (Class) type.getActualTypeArguments()[0];
    }

    public Message createMessage(Session session) throws JMSException {
        if (this.messageType.equals(TextMessage.class)) {
            this.message = session.createTextMessage();
        } else if (this.messageType.equals(MapMessage.class)) {
            this.message = session.createMapMessage();
        } else if (this.messageType.equals(BytesMessage.class)) {
            this.message = session.createBytesMessage();
        } else if (this.messageType.equals(ObjectMessage.class)) {
            this.message = session.createObjectMessage();
        } else if (this.messageType.equals(StreamMessage.class)) {
            this.message = session.createStreamMessage();
        } else if (this.messageType.equals(Message.class)) {
            this.message = session.createMessage();
        }

        this.setParams((T) this.message);
        return this.message;
    }

    public void setParams(T message) {
        this.message = message;
    }

    public Message getMessage() {
        return this.message;
    }

    public String toString() {
        return this.getClass().getName()
                + " - message: \n"
                + (this.message != null ? this.message.toString() : "message not yet created!");
    }
}