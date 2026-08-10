package com.bank.card_ops_producer.infrastructure.redis;

import com.bank.card_ops_producer.domain.port.AttemptStateRepository;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Repository;
import reactor.adapter.rxjava.RxJava3Adapter;

import java.time.Duration;

@Repository
@RequiredArgsConstructor
public class RedisAttemptStateRepository implements AttemptStateRepository {

    private final ReactiveStringRedisTemplate redis;

    @Override
    public Single<Boolean> existsByRequestId(String requestId) {
        return RxJava3Adapter.monoToSingle(redis.hasKey("card:req:" + requestId));
    }

    @Override
    public Single<Boolean> saveFirstAttempt(String requestId) {
        return RxJava3Adapter.monoToSingle(redis.opsForValue().set("card:req:" + requestId, "1"));
    }

    @Override
    public Single<Boolean> saveEventSnapshot(String requestId, String json, Duration ttl) {
        return RxJava3Adapter.monoToSingle(redis.opsForValue().set("card:event:" + requestId, json, ttl));
    }
}
