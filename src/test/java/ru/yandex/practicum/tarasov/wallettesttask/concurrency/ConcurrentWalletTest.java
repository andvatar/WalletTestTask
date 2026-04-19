package ru.yandex.practicum.tarasov.wallettesttask.concurrency;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.jdbc.Sql;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.OperationResponseDto;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.service.WalletMessageRouter;
import ru.yandex.practicum.tarasov.wallettesttask.entity.Wallet;
import ru.yandex.practicum.tarasov.wallettesttask.enums.Operations;
import ru.yandex.practicum.tarasov.wallettesttask.repository.WalletRepository;
import ru.yandex.practicum.tarasov.wallettesttask.service.IdempotencyService;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.Fail.fail;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@ActiveProfiles("test")
public class ConcurrentWalletTest {

    @Autowired
    private WalletRepository walletRepository;

    @Autowired
    private WalletMessageRouter walletMessageRouter;

    @Autowired
    private IdempotencyService idempotencyService;

    private final int numberOfThreads = 100;
    private final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

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

    @BeforeEach
    void warmUp() {
        idempotencyService.getResponse("1");
    }

    @Test
    @Sql(scripts = "/db/init.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = "/db/clear.sql",  executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void singleWalletTest() throws InterruptedException {

        int requestsPerThread = 100;

        Semaphore semaphore = new Semaphore(150);

        UUID walletId = UUID.fromString("2a038459-ab1f-4911-aa30-faecf993a8d0");

        CountDownLatch latch = new CountDownLatch(1);

        List<CompletableFuture<OperationResponseDto>> futures = new ArrayList<>();
        for (int i = 0; i < numberOfThreads * requestsPerThread; i++) {
            var future = new CompletableFuture<OperationResponseDto>();
            futures.add(future);
        }

        for (int i = 0; i < numberOfThreads; i++) {
            final int threadIndex = i;
            executor.submit(() -> {
                try{
                    latch.await();
                    for (int j = 0; j < requestsPerThread; j++) {

                        var future =  futures.get(threadIndex * requestsPerThread + j);

                        semaphore.acquire();

                        walletMessageRouter.route(walletId, BigDecimal.valueOf(100), Operations.DEPOSIT, UUID.randomUUID().toString(), future);

                        future.handle((res, ex) -> {
                            semaphore.release();
                            return null;
                        });
                    }
                }
                catch (InterruptedException e){
                    Thread.currentThread().interrupt();
                }
            });
        }

        Instant start = Instant.now();
        latch.countDown();

        CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        try {
            allOf.get(10, TimeUnit.SECONDS);
        } catch (TimeoutException | ExecutionException e) {
            fail("Timeout exception");
        }

        Instant end = Instant.now();

        System.out.println("Execution time: " + Duration.between(start, end).toMillis());

        assertThat(Duration.between(start, end).toMillis() < TimeUnit.SECONDS.toMillis(2)).isTrue();

        Wallet wallet = walletRepository.findById(walletId).orElseThrow();
        assertThat(wallet.getAmount().longValue()).isEqualTo(1_000_000);
    }

    @Test
    @Sql(scripts = "/db/init.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = "/db/clear.sql",  executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void singleWalletAddRemoveTest() throws InterruptedException {

        int requestsPerThread = 50;
        UUID walletId = UUID.fromString("2a038459-ab1f-4911-aa30-faecf993a8d0");

        Semaphore semaphore = new Semaphore(150);

        CountDownLatch latch = new CountDownLatch(1);

        List<CompletableFuture<OperationResponseDto>> futures = new ArrayList<>();
        for (int i = 0; i < numberOfThreads * requestsPerThread * 2; i++) {
            var future = new CompletableFuture<OperationResponseDto>();
            futures.add(future);
        }

        for (int i = 0; i < numberOfThreads; i++) {
            final int threadIndex = i;
            executor.submit(() -> {
                try{
                    latch.await();
                    for (int j = 0; j < requestsPerThread; j++) {

                        int baseIndex = (threadIndex * requestsPerThread * 2) + (j * 2);
                        var future1 =  futures.get(baseIndex);
                        var future2 =  futures.get(baseIndex + 1);

                        semaphore.acquire(2);

                        walletMessageRouter.route(walletId, BigDecimal.valueOf(100), Operations.DEPOSIT, UUID.randomUUID().toString(), future1);
                        walletMessageRouter.route(walletId, BigDecimal.valueOf(100), Operations.WITHDRAW, UUID.randomUUID().toString(), future2);

                        future1.handle((res, ex) -> {
                            semaphore.release();
                            return null;
                        });
                        future2.handle((res, ex) -> {
                            semaphore.release();
                            return null;
                        });

                    }
                }
                catch (InterruptedException e){
                    Thread.currentThread().interrupt();
                }
            });
        }

        Instant start = Instant.now();
        latch.countDown();

        CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        try {
            allOf.get(10, TimeUnit.SECONDS);
        } catch (TimeoutException | ExecutionException e) {
            fail("Timeout exception");
        }

        Instant end = Instant.now();

        System.out.println("Execution time: " + Duration.between(start, end).toMillis());

        assertThat(Duration.between(start, end).toMillis() < TimeUnit.SECONDS.toMillis(2)).isTrue();

        Wallet wallet = walletRepository.findById(walletId).orElseThrow();
        assertThat(wallet.getAmount().longValue()).isEqualTo(0);
    }

    @Test
    @Sql(scripts = "/db/init.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = "/db/clear.sql",  executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void manyWalletsTest() throws InterruptedException {

        int requestsPerThread = 10;
        UUID walletId0 = UUID.fromString("2a038459-ab1f-4911-aa30-faecf993a8d0");
        UUID walletId1 = UUID.fromString("2a038459-ab1f-4911-aa30-faecf993a8d1");
        UUID walletId2 = UUID.fromString("2a038459-ab1f-4911-aa30-faecf993a8d2");
        UUID walletId3 = UUID.fromString("2a038459-ab1f-4911-aa30-faecf993a8d3");
        UUID walletId4 = UUID.fromString("2a038459-ab1f-4911-aa30-faecf993a8d4");
        UUID walletId5 = UUID.fromString("2a038459-ab1f-4911-aa30-faecf993a8d5");
        UUID walletId6 = UUID.fromString("2a038459-ab1f-4911-aa30-faecf993a8d6");
        UUID walletId7 = UUID.fromString("2a038459-ab1f-4911-aa30-faecf993a8d7");
        UUID walletId8 = UUID.fromString("2a038459-ab1f-4911-aa30-faecf993a8d8");
        UUID walletId9 = UUID.fromString("2a038459-ab1f-4911-aa30-faecf993a8d9");

        Semaphore semaphore = new Semaphore(150);

        List<CompletableFuture<OperationResponseDto>> futures = new ArrayList<>();
        for (int i = 0; i < numberOfThreads * requestsPerThread * 10; i++) {
            var future = new CompletableFuture<OperationResponseDto>();
            futures.add(future);
        }

        CountDownLatch latch = new CountDownLatch(1);

        for (int i = 0; i < numberOfThreads; i++) {
            final int threadIndex = i;
            executor.submit(() -> {
                try{
                    latch.await();
                    for (int j = 0; j < requestsPerThread; j++) {

                        int baseIndex = (threadIndex * requestsPerThread * 10) + (j * 10);

                        var future1 = futures.get(baseIndex);
                        var future2 = futures.get(baseIndex+1);
                        var future3 = futures.get(baseIndex+2);
                        var future4 = futures.get(baseIndex+3);
                        var future5 = futures.get(baseIndex+4);
                        var future6 = futures.get(baseIndex+5);
                        var future7 = futures.get(baseIndex+6);
                        var future8 = futures.get(baseIndex+7);
                        var future9 = futures.get(baseIndex+8);
                        var future10 = futures.get(baseIndex+9);

                        semaphore.acquire(10);

                        walletMessageRouter.route(walletId0, BigDecimal.valueOf(100), Operations.DEPOSIT, UUID.randomUUID().toString(), future1);
                        walletMessageRouter.route(walletId1, BigDecimal.valueOf(100), Operations.DEPOSIT, UUID.randomUUID().toString(), future2);
                        walletMessageRouter.route(walletId2, BigDecimal.valueOf(100), Operations.DEPOSIT, UUID.randomUUID().toString(), future3);
                        walletMessageRouter.route(walletId3, BigDecimal.valueOf(100), Operations.DEPOSIT, UUID.randomUUID().toString(), future4);
                        walletMessageRouter.route(walletId4, BigDecimal.valueOf(100), Operations.DEPOSIT, UUID.randomUUID().toString(), future5);
                        walletMessageRouter.route(walletId5, BigDecimal.valueOf(100), Operations.DEPOSIT, UUID.randomUUID().toString(), future6);
                        walletMessageRouter.route(walletId6, BigDecimal.valueOf(100), Operations.DEPOSIT, UUID.randomUUID().toString(), future7);
                        walletMessageRouter.route(walletId7, BigDecimal.valueOf(100), Operations.DEPOSIT, UUID.randomUUID().toString(), future8);
                        walletMessageRouter.route(walletId8, BigDecimal.valueOf(100), Operations.DEPOSIT, UUID.randomUUID().toString(), future9);
                        walletMessageRouter.route(walletId9, BigDecimal.valueOf(100), Operations.DEPOSIT, UUID.randomUUID().toString(), future10);

                        future1.handle((res, ex) -> {
                            semaphore.release();
                            return null;
                        });

                        future2.handle((res, ex) -> {
                            semaphore.release();
                            return null;
                        });

                        future3.handle((res, ex) -> {
                            semaphore.release();
                            return null;
                        });

                        future4.handle((res, ex) -> {
                            semaphore.release();
                            return null;
                        });

                        future5.handle((res, ex) -> {
                            semaphore.release();
                            return null;
                        });

                        future6.handle((res, ex) -> {
                            semaphore.release();
                            return null;
                        });

                        future7.handle((res, ex) -> {
                            semaphore.release();
                            return null;
                        });

                        future8.handle((res, ex) -> {
                            semaphore.release();
                            return null;
                        });

                        future9.handle((res, ex) -> {
                            semaphore.release();
                            return null;
                        });

                        future10.handle((res, ex) -> {
                            semaphore.release();
                            return null;
                        });


                    }
                }
                catch (InterruptedException e){
                    Thread.currentThread().interrupt();
                }
            });
        }

        Instant start = Instant.now();
        latch.countDown();


        CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        try {
            allOf.get(10, TimeUnit.SECONDS);
        } catch (TimeoutException | ExecutionException e) {
            fail("Timeout exception");
        }

        Instant end = Instant.now();

        System.out.println("Execution time: " + Duration.between(start, end).toMillis());

        assertThat(Duration.between(start, end).toMillis() < TimeUnit.SECONDS.toMillis(2)).isTrue();

        Wallet wallet;

        wallet = walletRepository.findById(walletId0).orElseThrow();
        assertThat(wallet.getAmount().longValue()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId1).orElseThrow();
        assertThat(wallet.getAmount().longValue()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId2).orElseThrow();
        assertThat(wallet.getAmount().longValue()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId3).orElseThrow();
        assertThat(wallet.getAmount().longValue()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId4).orElseThrow();
        assertThat(wallet.getAmount().longValue()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId5).orElseThrow();
        assertThat(wallet.getAmount().longValue()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId6).orElseThrow();
        assertThat(wallet.getAmount().longValue()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId7).orElseThrow();
        assertThat(wallet.getAmount().longValue()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId8).orElseThrow();
        assertThat(wallet.getAmount().longValue()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId9).orElseThrow();
        assertThat(wallet.getAmount().longValue()).isEqualTo(100_000);
    }
}
