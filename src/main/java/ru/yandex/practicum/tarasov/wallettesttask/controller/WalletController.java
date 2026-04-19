package ru.yandex.practicum.tarasov.wallettesttask.controller;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.BalanceDto;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.OperationDto;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.OperationResponseDto;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.service.WalletMessageRouter;
import ru.yandex.practicum.tarasov.wallettesttask.enums.ErrorCode;
import ru.yandex.practicum.tarasov.wallettesttask.enums.OperationResults;
import ru.yandex.practicum.tarasov.wallettesttask.service.IdempotencyService;
import ru.yandex.practicum.tarasov.wallettesttask.service.RedisBalanceCheckService;
import ru.yandex.practicum.tarasov.wallettesttask.service.WalletService;
import ru.yandex.practicum.tarasov.wallettesttask.utility.WalletException;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/v1")
@Slf4j
public class WalletController {
    private final WalletService walletService;
    private final WalletMessageRouter walletMessageRouter;
    private final IdempotencyService idempotencyService;
    private final RedisBalanceCheckService redisBalanceCheckService;

    public WalletController(WalletService walletService,
                            WalletMessageRouter walletMessageRouter,
                            IdempotencyService idempotencyService,
                            RedisBalanceCheckService redisBalanceCheckService) {
        this.walletService = walletService;
        this.walletMessageRouter = walletMessageRouter;
        this.idempotencyService = idempotencyService;
        this.redisBalanceCheckService = redisBalanceCheckService;
    }

    @PostMapping("/wallet")
    public CompletableFuture<OperationResponseDto> walletOperation(@Valid @RequestBody OperationDto operationDto,
                                                                   @RequestHeader("Idempotency-Key") String idempotencyKey) {

        OperationResponseDto inProgress = new OperationResponseDto(
                operationDto.walletId(),
                null,
                null,
                OperationResults.IN_PROGRESS
                );

        Optional<OperationResponseDto> existingStatus = idempotencyService.reserveOrGet(idempotencyKey, inProgress);

        CompletableFuture<OperationResponseDto> future = new CompletableFuture<>();

        if (existingStatus.isPresent()) {
            OperationResponseDto cached = existingStatus.get();
            if (cached.result().equals(OperationResults.IN_PROGRESS)) {
                future.completeExceptionally(new WalletException(ErrorCode.OPERATION_IN_PROGRESS));
            } else {
                future.complete(cached);
            }
            return future;
        }

        walletMessageRouter.route(
                operationDto.walletId(),
                operationDto.amount(),
                operationDto.operationType(),
                idempotencyKey,
                future
        );

        return future;
    }

    @GetMapping("/wallets/{WALLET_ID}")
    public BalanceDto getBalance(@PathVariable("WALLET_ID") UUID walletId) {
        Optional<BalanceDto> optionalBalanceDto = redisBalanceCheckService.getBalance(walletId);
        return optionalBalanceDto.orElseGet(() -> walletService.getBalance(walletId));

    }
}
