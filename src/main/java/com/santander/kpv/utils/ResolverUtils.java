package com.santander.kpv.utils;

import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import org.springframework.jms.support.destination.DestinationResolver;

public class ResolverUtils implements DestinationResolver {
    @Override
    public Destination resolveDestinationName(Session session, String destinationName, boolean pubSubDomain) throws JMSException {
        if (pubSubDomain) {
            return session.createTopic(destinationName);
        } else {
            return session.createQueue(destinationName);
        }
    }
}
