package org.springframework.boot.autoconfigure.amqp;

import org.springframework.amqp.rabbit.connection.SimpleRoutingConnectionFactory;

public class DefaultRoutingConnectionFactory extends SimpleRoutingConnectionFactory {

    public DefaultRoutingConnectionFactory() {
        setConsistentConfirmsReturns(false);
    }

    @Override
    public boolean isPublisherConfirms() {
        return determineTargetConnectionFactory().isPublisherConfirms();
    }

    @Override
    public boolean isPublisherReturns() {
        return determineTargetConnectionFactory().isPublisherConfirms();
    }

    @Override
    public boolean isSimplePublisherConfirms() {
        return determineTargetConnectionFactory().isPublisherConfirms();
    }
}
