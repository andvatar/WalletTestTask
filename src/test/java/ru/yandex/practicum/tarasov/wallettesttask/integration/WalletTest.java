package ru.yandex.practicum.tarasov.wallettesttask.integration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.jdbc.Sql;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.ErrorDto;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.OperationDto;
import ru.yandex.practicum.tarasov.wallettesttask.enums.ErrorCode;
import ru.yandex.practicum.tarasov.wallettesttask.enums.Operations;
import ru.yandex.practicum.tarasov.wallettesttask.repository.WalletRepository;
import ru.yandex.practicum.tarasov.wallettesttask.units.WalletException;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@ActiveProfiles("test")
public class WalletTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private WalletRepository walletRepository;

    @LocalServerPort
    private int port;

    @Container
    static PostgreSQLContainer<?> postgreSQLContainer = new PostgreSQLContainer<>("postgres:18")
            .withDatabaseName("wallet_db")
            .withUsername("test_user")
            .withPassword("test_pass");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgreSQLContainer::getJdbcUrl);
        registry.add("spring.datasource.username", postgreSQLContainer::getUsername);
        registry.add("spring.datasource.password", postgreSQLContainer::getPassword);
    }


    @Test
    @Sql(scripts = "/db/init.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = "/db/clear.sql",  executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void addMoney() {
        UUID walletId = UUID.fromString("2a038459-ab1f-4911-aa30-faecf993a8d0");

        OperationDto operationDto = new OperationDto(
                walletId,
                Operations.DEPOSIT,
                100);
        ResponseEntity<Void> response = restTemplate.postForEntity("http://localhost:" + port + "/api/v1/wallet", operationDto, Void.class);

        var wallet = walletRepository
                .findById(walletId)
                .orElseThrow(() -> new WalletException(ErrorCode.WALLET_NOT_FOUND, walletId));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(wallet.getAmount()).isEqualTo(operationDto.amount());
    }

    @Test
    @Sql(scripts = "/db/init.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = "/db/clear.sql",  executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void removeMoney() {
        UUID walletId = UUID.fromString("a40e3b60-ffe9-4c2c-8bd4-8c2ac1dd9c03");

        OperationDto operationDto = new OperationDto(
                walletId,
                Operations.WITHDRAW,
                400);
        ResponseEntity<Void> response = restTemplate.postForEntity("http://localhost:" + port + "/api/v1/wallet", operationDto, Void.class);

        var wallet = walletRepository
                .findById(walletId)
                .orElseThrow(() -> new WalletException(ErrorCode.WALLET_NOT_FOUND, walletId));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(wallet.getAmount()).isEqualTo(600);
    }

    @Test
    @Sql(scripts = "/db/init.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = "/db/clear.sql",  executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void removeTooMuchMoney() {
        UUID walletId = UUID.fromString("a40e3b60-ffe9-4c2c-8bd4-8c2ac1dd9c03");

        OperationDto operationDto = new OperationDto(
                walletId,
                Operations.WITHDRAW,
                4000);
        ResponseEntity<ErrorDto> response = restTemplate.postForEntity("http://localhost:" + port + "/api/v1/wallet", operationDto, ErrorDto.class);

        assertThat(response.getStatusCode().value()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(response.getBody().errorCode().equals(ErrorCode.INSUFFICIENT_BALANCE.toString()));
    }

    @Test
    @Sql(scripts = "/db/init.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = "/db/clear.sql",  executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void addToWrongWallet() {
        UUID walletId = UUID.fromString("a40e3b60-ffe9-4c2c-8bd4-8c2ac1dd9c04");

        OperationDto operationDto = new OperationDto(
                walletId,
                Operations.DEPOSIT,
                4000);
        ResponseEntity<ErrorDto> response = restTemplate.postForEntity("http://localhost:" + port + "/api/v1/wallet", operationDto, ErrorDto.class);

        assertThat(response.getStatusCode().value()).isEqualTo(HttpStatus.NOT_FOUND.value());
        assertThat(response.getBody().errorCode().equals(ErrorCode.WALLET_NOT_FOUND.toString()));
    }

    @Test
    @Sql(scripts = "/db/init.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = "/db/clear.sql",  executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void removeFromWrongWallet() {
        UUID walletId = UUID.fromString("a40e3b60-ffe9-4c2c-8bd4-8c2ac1dd9c04");

        OperationDto operationDto = new OperationDto(
                walletId,
                Operations.WITHDRAW,
                4000);
        ResponseEntity<ErrorDto> response = restTemplate.postForEntity("http://localhost:" + port + "/api/v1/wallet", operationDto, ErrorDto.class);

        assertThat(response.getStatusCode().value()).isEqualTo(HttpStatus.NOT_FOUND.value());
        assertThat(response.getBody().errorCode().equals(ErrorCode.WALLET_NOT_FOUND.toString()));
    }
}
