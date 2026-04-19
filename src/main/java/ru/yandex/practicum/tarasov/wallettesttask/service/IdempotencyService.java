package ru.yandex.practicum.tarasov.wallettesttask.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.OperationResponseDto;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Service
@Slf4j
public class IdempotencyService {
    private final RedisTemplate<String, OperationResponseDto> redisTemplate;
    private final String idempotencyKeyPrefix = "idempotencyKey:";

    public IdempotencyService(@Qualifier("redisIdempotencyTemplate") RedisTemplate<String, OperationResponseDto> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void addKey(String idempotencyKey, OperationResponseDto operationResponseDto) {
        String key = idempotencyKeyPrefix + idempotencyKey;
        redisTemplate.opsForValue().set(key, operationResponseDto, Duration.ofSeconds(10));

    }

    public void addKeys(Map<String, OperationResponseDto> operationResponses) {
        var start = Instant.now();
        var keySerializer = (StringRedisSerializer) redisTemplate.getKeySerializer();
        var valueSerializer = (Jackson2JsonRedisSerializer<OperationResponseDto>) redisTemplate.getValueSerializer();

        log.info("operationResponses: {}", operationResponses);

        Map<byte[], byte[]> rawData = new HashMap<>(operationResponses.size());
        for (Map.Entry<String, OperationResponseDto> entry : operationResponses.entrySet()) {
            rawData.put(
                    keySerializer.serialize(idempotencyKeyPrefix + entry.getKey()),
                    valueSerializer.serialize(entry.getValue())
            );
        }

        redisTemplate.executePipelined((RedisConnection connection) -> {
            for (Map.Entry<byte[], byte[]> entry : rawData.entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    connection.stringCommands().set(
                            entry.getKey(),
                            entry.getValue(),
                            Expiration.seconds(10),
                            RedisStringCommands.SetOption.upsert()
                    );
                }
            }
            return null;
        });

        var end = Instant.now();
        if(Duration.between(start, end).toMillis() > 5) {
            log.info("addKeys: {} ms", Duration.between(start, end).toMillis());
        }
    }

    public Optional<OperationResponseDto> getResponse(String idempotencyKey) {
        String key = idempotencyKeyPrefix + idempotencyKey;
        return Optional.ofNullable(redisTemplate.opsForValue().get(key));
    }

    public Optional<OperationResponseDto> reserveOrGet(String key, OperationResponseDto inProgressDto) {
        key = idempotencyKeyPrefix + key;
        boolean created = Boolean.TRUE.equals(redisTemplate.opsForValue()
                .setIfAbsent(key, inProgressDto, Duration.ofSeconds(10)));

        if (created) {
            return Optional.empty();
        } else {
            return Optional.ofNullable(redisTemplate.opsForValue().get(key));
        }
    }
}
