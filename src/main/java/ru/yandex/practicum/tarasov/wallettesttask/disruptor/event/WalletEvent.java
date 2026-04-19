package ru.yandex.practicum.tarasov.wallettesttask.disruptor.event;

import lombok.Data;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.OperationResponseDto;
import ru.yandex.practicum.tarasov.wallettesttask.enums.Operations;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Data
public class WalletEvent {
    private UUID walletId;
    private Operations operation;
    private BigDecimal amount;
    private String idempotencyKey;
    private CompletableFuture<OperationResponseDto> future;

    public void clear() {
        this.walletId = null;
        this.operation = null;
        this.future = null;
        this.idempotencyKey = null;
        this.amount = null;
    }
}
