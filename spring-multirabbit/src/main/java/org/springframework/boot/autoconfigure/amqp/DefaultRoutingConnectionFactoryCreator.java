package org.springframework.boot.autoconfigure.amqp;

import org.springframework.amqp.rabbit.connection.AbstractRoutingConnectionFactory;

public class DefaultRoutingConnectionFactoryCreator implements RoutingConnectionFactoryCreator {
    @Override
    public AbstractRoutingConnectionFactory create() {
        return new DefaultRoutingConnectionFactory();
    }
}
