package ru.yandex.practicum.tarasov.wallettesttask.disruptor.event;

import lombok.Data;
import ru.yandex.practicum.tarasov.wallettesttask.enums.Operations;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Data
public class WalletEvent {
    private UUID walletId;
    private Operations operation;
    private long amount;
    private CompletableFuture<Void> future;

    public void clear() {
        this.walletId = null;
        this.operation = null;
        this.future = null;
    }
}
