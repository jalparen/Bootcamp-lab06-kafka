package com.bank.card_ops_producer.domain.policy;

import com.bank.card_ops_producer.api.dto.CardReplacementRequestDto;
import io.reactivex.rxjava3.core.Single;
import org.springframework.stereotype.Component;

@Component
public class SecondTimePolicy implements AttemptPolicy {

    @Override
    public Single<Integer> resolveAttempt(CardReplacementRequestDto dto) {
        return Single.just(2);
    }

    @Override
    public String name() {
        return "secondTimePolicy";
    }
}
