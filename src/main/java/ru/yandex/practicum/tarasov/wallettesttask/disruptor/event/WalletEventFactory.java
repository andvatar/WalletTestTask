package ru.yandex.practicum.tarasov.wallettesttask.disruptor.event;

import com.lmax.disruptor.EventFactory;

public class WalletEventFactory implements EventFactory<WalletEvent> {
    public WalletEvent newInstance() {
        return new WalletEvent();
    }
}
