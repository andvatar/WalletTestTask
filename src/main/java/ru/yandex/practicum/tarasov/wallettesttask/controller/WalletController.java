package ru.yandex.practicum.tarasov.wallettesttask.controller;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.BalanceDto;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.OperationDto;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.service.WalletMessageRouter;
import ru.yandex.practicum.tarasov.wallettesttask.service.WalletService;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/v1")
@Slf4j
public class WalletController {
    private final WalletService walletService;
    private final WalletMessageRouter walletMessageRouter;

    public WalletController(WalletService walletService, WalletMessageRouter walletMessageRouter) {
        this.walletService = walletService;
        this.walletMessageRouter = walletMessageRouter;
    }

    @PostMapping("/wallet")
    public CompletableFuture<Void> walletOperation(@Valid @RequestBody OperationDto operationDto) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        walletMessageRouter.route(
                operationDto.walletId(),
                operationDto.amount(),
                operationDto.operationType(),
                future
                );
        return future;
    }

    @GetMapping("/wallets/{WALLET_ID}")
    public BalanceDto getBalance(@PathVariable("WALLET_ID") UUID walletId) {
        return walletService.getBalance(walletId);
    }
}
