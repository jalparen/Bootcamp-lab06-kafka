package com.bank.card_ops_producer.domain.policy;

import com.bank.card_ops_producer.api.dto.CardReplacementRequestDto;
import com.bank.card_ops_producer.domain.port.AttemptStateRepository;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Component
@Primary
@RequiredArgsConstructor
public class AttemptPolicyOrchestrator implements AttemptPolicy {

    private final FirstTimePolicy first;
    private final SecondTimePolicy second;
    private final AttemptStateRepository repo;

    @Override
    public Single<Integer> resolveAttempt(CardReplacementRequestDto dto) {
        final String id = dto.getRequestId();
        return repo.existsByRequestId(id)
                .flatMap(exists -> exists
                        ? second.resolveAttempt(dto)
                        : repo.saveFirstAttempt(id)
                                .flatMap(ok -> first.resolveAttempt(dto)));
    }

    @Override
    public String name() {
        return "orchestrator";
    }
}
