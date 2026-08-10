package com.bank.dispatch_consumer.infrastructure.kafka;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import reactor.kafka.receiver.ReceiverOffset;

@Getter
@RequiredArgsConstructor
public class EventMessage<T> {

    private final T payload;
    private final ReceiverOffset offset;
    private final Object rawKafkaValue;

    public void ack() {
        offset.acknowledge();
    }
}
