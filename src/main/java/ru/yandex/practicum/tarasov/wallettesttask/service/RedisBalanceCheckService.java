package ru.yandex.practicum.tarasov.wallettesttask.service;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.BalanceDto;

import java.util.*;

@Service
public class RedisBalanceCheckService {
    private final RedisTemplate<String, BalanceDto> redisTemplate;
    private final String CACHE_KEY_PREFIX = "wallet:balance:";

    public RedisBalanceCheckService(@Qualifier("redisBalanceTemplate") RedisTemplate<String, BalanceDto> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void updateBalance(BalanceDto balanceDto) {
        String key = CACHE_KEY_PREFIX + balanceDto.id().toString();
        redisTemplate.opsForValue().set(key, balanceDto);
    }

    public void updateBalances(Set<BalanceDto> balances) {
        var keySerializer = (StringRedisSerializer) redisTemplate.getKeySerializer();
        var valueSerializer = (Jackson2JsonRedisSerializer<BalanceDto>) redisTemplate.getValueSerializer();

        Map<byte[], byte[]> rawData = new HashMap<>(balances.size());
        for (BalanceDto dto : balances) {
            rawData.put(
                    keySerializer.serialize(CACHE_KEY_PREFIX + dto.id().toString()),
                    valueSerializer.serialize(dto)
            );
        }

        redisTemplate.executePipelined((RedisConnection connection) -> {
            for (Map.Entry<byte[], byte[]> entry: rawData.entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    connection.stringCommands().set(
                            entry.getKey(),
                            entry.getValue(),
                            Expiration.seconds(120),
                            RedisStringCommands.SetOption.upsert()
                    );
                }
            }
            return null;
        });
    }

    public Optional<BalanceDto> getBalance(UUID id) {
        String key = CACHE_KEY_PREFIX + id.toString();
        return Optional.ofNullable(redisTemplate.opsForValue().get(key));
    }
}
