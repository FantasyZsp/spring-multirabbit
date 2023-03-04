package org.springframework.boot.autoconfigure.amqp;

import org.springframework.amqp.rabbit.connection.AbstractRoutingConnectionFactory;

public interface RoutingConnectionFactoryCreator {
    AbstractRoutingConnectionFactory create();
}
