package com.bank.card_ops_producer.infrastructure.kafka;

import com.bank.card_ops_producer.domain.port.EventPublisher;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component("kafkaEventPublisher")
@RequiredArgsConstructor
public class KafkaEventPublisher implements EventPublisher<Object> {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public Single<SendResult<String, Object>> publish(String topic, String key, Object value) {
        return Single.create(emitter ->
                kafkaTemplate.send(topic, key, value).whenComplete((result, ex) -> {
                    if (ex != null) {
                        emitter.onError(ex);
                    } else {
                        emitter.onSuccess(result);
                    }
                })
        );
    }
}
