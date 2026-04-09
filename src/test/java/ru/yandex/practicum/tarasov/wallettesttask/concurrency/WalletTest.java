package ru.yandex.practicum.tarasov.wallettesttask.concurrency;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.jdbc.Sql;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.service.WalletMessageRouter;
import ru.yandex.practicum.tarasov.wallettesttask.entity.Wallet;
import ru.yandex.practicum.tarasov.wallettesttask.enums.Operations;
import ru.yandex.practicum.tarasov.wallettesttask.repository.WalletRepository;

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
public class WalletTest {

    @Autowired
    private WalletRepository walletRepository;

    @Autowired
    private WalletMessageRouter walletMessageRouter;

    private final int numberOfThreads = 100;
    private final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

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
    void singleWalletTest() throws InterruptedException {
        int requestsPerThread = 100;

        Semaphore semaphore = new Semaphore(150);

        UUID walletId = UUID.fromString("2a038459-ab1f-4911-aa30-faecf993a8d0");

        CountDownLatch latch = new CountDownLatch(1);

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < numberOfThreads * requestsPerThread; i++) {
            var future = new CompletableFuture<Void>();
            futures.add(future);
        }

        for (int i = 0; i < numberOfThreads; i++) {
            final int threadIndex = i;
            executor.submit(() -> {
                try{
                    latch.await();
                    for (int j = 0; j < requestsPerThread; j++) {

                        var future =  futures.get(threadIndex * requestsPerThread + j);
                        walletMessageRouter.route(walletId, 100, Operations.DEPOSIT, future);
                        semaphore.acquire();

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

        Wallet wallet = walletRepository.findById(walletId).orElseThrow();
        assertThat(wallet.getAmount()).isEqualTo(1_000_000);
    }

    @Test
    @Sql(scripts = "/db/init.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = "/db/clear.sql",  executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void singleWalletAddRemoveTest() throws InterruptedException {
        int requestsPerThread = 50;
        UUID walletId = UUID.fromString("2a038459-ab1f-4911-aa30-faecf993a8d0");

        Semaphore semaphore = new Semaphore(150);

        CountDownLatch latch = new CountDownLatch(1);

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < numberOfThreads * requestsPerThread; i++) {
            var future = new CompletableFuture<Void>();
            futures.add(future);
        }

        for (int i = 0; i < numberOfThreads; i++) {
            final int threadIndex = i;
            executor.submit(() -> {
                try{
                    latch.await();
                    for (int j = 0; j < requestsPerThread; j++) {

                        var future =  futures.get(threadIndex * requestsPerThread + j);
                        walletMessageRouter.route(walletId, 100, Operations.DEPOSIT, future);
                        walletMessageRouter.route(walletId, 100, Operations.WITHDRAW, future);
                        semaphore.acquire();

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

        Wallet wallet = walletRepository.findById(walletId).orElseThrow();
        assertThat(wallet.getAmount()).isEqualTo(0);
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

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < numberOfThreads * requestsPerThread; i++) {
            var future = new CompletableFuture<Void>();
            futures.add(future);
        }

        CountDownLatch latch = new CountDownLatch(1);

        for (int i = 0; i < numberOfThreads; i++) {
            final int threadIndex = i;
            executor.submit(() -> {
                try{
                    latch.await();
                    for (int j = 0; j < requestsPerThread; j++) {

                        var future =  futures.get(threadIndex * requestsPerThread + j);
                        walletMessageRouter.route(walletId0, 100, Operations.DEPOSIT, future);
                        walletMessageRouter.route(walletId1, 100, Operations.DEPOSIT, future);
                        walletMessageRouter.route(walletId2, 100, Operations.DEPOSIT, future);
                        walletMessageRouter.route(walletId3, 100, Operations.DEPOSIT, future);
                        walletMessageRouter.route(walletId4, 100, Operations.DEPOSIT, future);
                        walletMessageRouter.route(walletId5, 100, Operations.DEPOSIT, future);
                        walletMessageRouter.route(walletId6, 100, Operations.DEPOSIT, future);
                        walletMessageRouter.route(walletId7, 100, Operations.DEPOSIT, future);
                        walletMessageRouter.route(walletId8, 100, Operations.DEPOSIT, future);
                        walletMessageRouter.route(walletId9, 100, Operations.DEPOSIT, future);
                        semaphore.acquire();

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

        Wallet wallet;

        wallet = walletRepository.findById(walletId0).orElseThrow();
        assertThat(wallet.getAmount()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId1).orElseThrow();
        assertThat(wallet.getAmount()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId2).orElseThrow();
        assertThat(wallet.getAmount()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId3).orElseThrow();
        assertThat(wallet.getAmount()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId4).orElseThrow();
        assertThat(wallet.getAmount()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId5).orElseThrow();
        assertThat(wallet.getAmount()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId6).orElseThrow();
        assertThat(wallet.getAmount()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId7).orElseThrow();
        assertThat(wallet.getAmount()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId8).orElseThrow();
        assertThat(wallet.getAmount()).isEqualTo(100_000);

        wallet = walletRepository.findById(walletId9).orElseThrow();
        assertThat(wallet.getAmount()).isEqualTo(100_000);
    }
}
