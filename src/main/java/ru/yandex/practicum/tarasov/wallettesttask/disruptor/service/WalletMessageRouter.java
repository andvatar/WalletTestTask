package ru.yandex.practicum.tarasov.wallettesttask.disruptor.service;

import com.lmax.disruptor.RingBuffer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.event.WalletEvent;
import ru.yandex.practicum.tarasov.wallettesttask.enums.Operations;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class WalletMessageRouter {
    private final List<RingBuffer<WalletEvent>> ringBuffers;

    public WalletMessageRouter(List<RingBuffer<WalletEvent>> ringBuffers) {
        this.ringBuffers = ringBuffers;
    }

    public void route(UUID walletId,
                      long amount,
                      Operations operation,
                      CompletableFuture<Void> future) {
        int shardIndex = Math.abs(walletId.hashCode()) % this.ringBuffers.size();
        log.trace("Shard Index: {} walletId: {}", shardIndex, walletId);
        ringBuffers.get(shardIndex).publishEvent((event, sequence) -> {
            event.setWalletId(walletId);
            event.setAmount(amount);
            event.setOperation(operation);
            event.setFuture(future);
        });
    }
}
