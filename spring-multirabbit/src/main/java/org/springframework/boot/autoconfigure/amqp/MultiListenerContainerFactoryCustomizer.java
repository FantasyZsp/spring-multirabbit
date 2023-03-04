package org.springframework.boot.autoconfigure.amqp;

import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;

public interface MultiListenerContainerFactoryCustomizer {

    void configure(SimpleRabbitListenerContainerFactory containerFactory, String connectionName);
}
