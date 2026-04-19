package ru.yandex.practicum.tarasov.wallettesttask.disruptor.handler;

import com.lmax.disruptor.EventHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.event.WalletEvent;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.service.WalletProcessService;

import java.util.*;

@Getter
@Slf4j
public class WalletEventProcessor implements EventHandler<WalletEvent> {
    private final List<WalletEvent> walletEvents;
    private final int workerNumber;
    private final WalletProcessService walletProcessService;

    public WalletEventProcessor(int workerNumber,
                                WalletProcessService walletProcessService) {
        this.walletEvents = new ArrayList<>();
        this.workerNumber = workerNumber;
        this.walletProcessService = walletProcessService;
    }

    @Override
    public void onEvent(WalletEvent event, long sequence, boolean endOfBatch) {

        walletEvents.add(event);

        if(endOfBatch) {
            try {
                log.info("bufferSize: {}", walletEvents.size());
                walletProcessService.processWalletEvents(walletEvents);
            }
            finally {
                walletEvents.forEach(WalletEvent::clear);
                walletEvents.clear();
            }
        }
    }


}
