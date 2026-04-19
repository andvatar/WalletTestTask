package ru.yandex.practicum.tarasov.wallettesttask.integration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.*;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.jdbc.Sql;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.ErrorDto;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.OperationDto;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.OperationResponseDto;
import ru.yandex.practicum.tarasov.wallettesttask.enums.ErrorCode;
import ru.yandex.practicum.tarasov.wallettesttask.enums.Operations;
import ru.yandex.practicum.tarasov.wallettesttask.repository.WalletRepository;
import ru.yandex.practicum.tarasov.wallettesttask.utility.WalletException;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@ActiveProfiles("test")
public class IntegrationWalletTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private WalletRepository walletRepository;

    @Autowired
    @Qualifier("redisIdempotencyTemplate")
    private RedisTemplate<String, OperationResponseDto> redisIdempotencyTemplate;

    @LocalServerPort
    private int port;

    private final static HttpHeaders headers = new HttpHeaders();

    @Container
    static final PostgreSQLContainer<?> postgreSQLContainer = new PostgreSQLContainer<>("postgres:18")
            .withDatabaseName("wallet_db")
            .withUsername("test_user")
            .withPassword("test_pass");

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgreSQLContainer::getJdbcUrl);
        registry.add("spring.datasource.username", postgreSQLContainer::getUsername);
        registry.add("spring.datasource.password", postgreSQLContainer::getPassword);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @BeforeAll
    static void addHeader() {
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", "test");
    }

    @BeforeEach
    void clearCache() {
        redisIdempotencyTemplate.delete("idempotencyKey:test");
    }


    @Test
    @Sql(scripts = "/db/init.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = "/db/clear.sql",  executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void addMoney() {
        UUID walletId = UUID.fromString("2a038459-ab1f-4911-aa30-faecf993a8d0");

        OperationDto operationDto = new OperationDto(
                walletId,
                Operations.DEPOSIT,
                BigDecimal.valueOf(100));
        HttpEntity<OperationDto> requestEntity = new HttpEntity<>(operationDto, headers);
        ResponseEntity<OperationResponseDto> response = restTemplate.postForEntity("http://localhost:" + port + "/api/v1/wallet", requestEntity, OperationResponseDto.class);

        var wallet = walletRepository
                .findById(walletId)
                .orElseThrow(() -> new WalletException(ErrorCode.WALLET_NOT_FOUND, walletId));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(wallet.getAmount().longValue()).isEqualTo(operationDto.amount().longValue());
    }

    @Test
    @Sql(scripts = "/db/init.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = "/db/clear.sql",  executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void removeMoney() {
        UUID walletId = UUID.fromString("a40e3b60-ffe9-4c2c-8bd4-8c2ac1dd9c03");

        OperationDto operationDto = new OperationDto(
                walletId,
                Operations.WITHDRAW,
                BigDecimal.valueOf(400));
        HttpEntity<OperationDto> requestEntity = new HttpEntity<>(operationDto, headers);
        ResponseEntity<OperationResponseDto> response = restTemplate.postForEntity("http://localhost:" + port + "/api/v1/wallet", requestEntity, OperationResponseDto.class);

        var wallet = walletRepository
                .findById(walletId)
                .orElseThrow(() -> new WalletException(ErrorCode.WALLET_NOT_FOUND, walletId));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(wallet.getAmount().longValue()).isEqualTo(600);
    }

    @Test
    @Sql(scripts = "/db/init.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = "/db/clear.sql",  executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void removeTooMuchMoney() {
        UUID walletId = UUID.fromString("a40e3b60-ffe9-4c2c-8bd4-8c2ac1dd9c03");

        OperationDto operationDto = new OperationDto(
                walletId,
                Operations.WITHDRAW,
                BigDecimal.valueOf(4000));

        HttpEntity<OperationDto> requestEntity = new HttpEntity<>(operationDto, headers);
        ResponseEntity<ErrorDto> response = restTemplate.postForEntity("http://localhost:" + port + "/api/v1/wallet", requestEntity, ErrorDto.class);

        assertThat(response.getStatusCode().value()).isEqualTo(ErrorCode.INSUFFICIENT_BALANCE.getHttpStatus().value());
        assertThat(response.getBody().errorCode().equals(ErrorCode.INSUFFICIENT_BALANCE.toString()));
    }

    @Test
    @Sql(scripts = "/db/init.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = "/db/clear.sql",  executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void addToNotExistingWallet() {
        UUID walletId = UUID.fromString("a40e3b60-ffe9-4c2c-8bd4-8c2ac1dd9c04");

        OperationDto operationDto = new OperationDto(
                walletId,
                Operations.DEPOSIT,
                BigDecimal.valueOf(4000));
        HttpEntity<OperationDto> requestEntity = new HttpEntity<>(operationDto, headers);

        ResponseEntity<ErrorDto> response = restTemplate.postForEntity("http://localhost:" + port + "/api/v1/wallet", requestEntity, ErrorDto.class);

        assertThat(response.getStatusCode().value()).isEqualTo(ErrorCode.WALLET_NOT_FOUND.getHttpStatus().value());
        assertThat(response.getBody().errorCode().equals(ErrorCode.WALLET_NOT_FOUND.toString()));
    }

    @Test
    @Sql(scripts = "/db/init.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = "/db/clear.sql",  executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void removeFromNotExistingWallet() {
        UUID walletId = UUID.fromString("a40e3b60-ffe9-4c2c-8bd4-8c2ac1dd9c04");

        OperationDto operationDto = new OperationDto(
                walletId,
                Operations.WITHDRAW,
                BigDecimal.valueOf(4000));
        HttpEntity<OperationDto> requestEntity = new HttpEntity<>(operationDto, headers);
        ResponseEntity<ErrorDto> response = restTemplate.postForEntity("http://localhost:" + port + "/api/v1/wallet", requestEntity, ErrorDto.class);

        assertThat(response.getStatusCode().value()).isEqualTo(ErrorCode.WALLET_NOT_FOUND.getHttpStatus().value());
        assertThat(response.getBody().errorCode().equals(ErrorCode.WALLET_NOT_FOUND.toString()));
    }
}
