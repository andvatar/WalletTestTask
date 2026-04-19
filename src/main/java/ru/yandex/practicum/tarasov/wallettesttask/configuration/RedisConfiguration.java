package ru.yandex.practicum.tarasov.wallettesttask.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.BalanceDto;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.OperationResponseDto;

@Configuration
public class RedisConfiguration {
    @Bean
    public RedisTemplate<String, OperationResponseDto> redisIdempotencyTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, OperationResponseDto> redisTemplate = new RedisTemplate<>();

        Jackson2JsonRedisSerializer<OperationResponseDto> serializer = new Jackson2JsonRedisSerializer<>(OperationResponseDto.class);
        return setTemplateParams(redisTemplate,  connectionFactory, serializer);
    }

    @Bean
    public RedisTemplate<String, BalanceDto> redisBalanceTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, BalanceDto> redisTemplate = new RedisTemplate<>();
        Jackson2JsonRedisSerializer<BalanceDto> serializer = new Jackson2JsonRedisSerializer<>(BalanceDto.class);
        return setTemplateParams(redisTemplate,  connectionFactory, serializer);
    }

    private <K, V> RedisTemplate<K, V> setTemplateParams(RedisTemplate<K, V> redisTemplate, RedisConnectionFactory connectionFactory, Jackson2JsonRedisSerializer<?> serializer) {
        redisTemplate.setConnectionFactory(connectionFactory);

        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashKeySerializer(new StringRedisSerializer());

        redisTemplate.setValueSerializer(serializer);
        redisTemplate.setHashValueSerializer(serializer);

        redisTemplate.afterPropertiesSet();

        return redisTemplate;
    }
}
