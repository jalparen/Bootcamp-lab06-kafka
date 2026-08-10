package com.bank.card_ops_producer.domain.port;

import io.reactivex.rxjava3.core.Single;

import java.time.Duration;

public interface AttemptStateRepository {

    Single<Boolean> existsByRequestId(String requestId);

    Single<Boolean> saveFirstAttempt(String requestId);

    Single<Boolean> saveEventSnapshot(String requestId, String json, Duration ttl);
}
