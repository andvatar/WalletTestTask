package ru.yandex.practicum.tarasov.wallettesttask.disruptor.handler;

import com.lmax.disruptor.EventHandler;
import lombok.Getter;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.event.WalletEvent;
import ru.yandex.practicum.tarasov.wallettesttask.entity.Wallet;
import ru.yandex.practicum.tarasov.wallettesttask.enums.ErrorCode;
import ru.yandex.practicum.tarasov.wallettesttask.enums.Operations;
import ru.yandex.practicum.tarasov.wallettesttask.repository.WalletRepository;
import ru.yandex.practicum.tarasov.wallettesttask.units.WalletException;

import java.util.*;
import java.util.stream.Collectors;

@Getter
public class WalletEventProcessor implements EventHandler<WalletEvent> {
    private final List<WalletEvent> walletEvents;
    private final WalletRepository walletRepository;
    private final int workerNumber;

    public WalletEventProcessor(int workerNumber, WalletRepository walletRepository) {
        this.walletEvents = new ArrayList<>();
        this.walletRepository = walletRepository;
        this.workerNumber = workerNumber;
    }

    @Override
    public void onEvent(WalletEvent event, long sequence, boolean endOfBatch) {
        walletEvents.add(event);

        if(endOfBatch) {
            try {
                processWalletEvents(walletEvents);
            }
            finally {
                walletEvents.forEach(WalletEvent::clear);
                walletEvents.clear();
            }
        }
    }

    private void processWalletEvents(List<WalletEvent> walletEvents) {
        Set<UUID> walletIds = walletEvents.stream().map(WalletEvent::getWalletId).collect(Collectors.toSet());

        Map<UUID, Wallet> walletMap = walletRepository
                .findAllById(walletIds)
                .stream()
                .collect(Collectors.toMap(Wallet::getId, wallet -> wallet));

        List<WalletEvent> validEvents = new ArrayList<>();

        for(WalletEvent walletEvent : walletEvents) {
            if(walletMap.containsKey(walletEvent.getWalletId())) {
                validEvents.add(walletEvent);
            }
            else {
                walletEvent.getFuture().completeExceptionally(new WalletException(ErrorCode.WALLET_NOT_FOUND, walletEvent.getWalletId()));
            }
        }

        Set<Wallet> walletsToSave = new HashSet<>();
        List<WalletEvent> eventsToSave = new ArrayList<>();

        for(WalletEvent walletEvent : validEvents) {
            try{
                Wallet wallet = walletMap.get(walletEvent.getWalletId());
                if(walletEvent.getOperation().equals(Operations.DEPOSIT))
                    wallet.setAmount(wallet.getAmount() + walletEvent.getAmount());
                else {
                    if(wallet.getAmount() >= walletEvent.getAmount())
                        wallet.setAmount(wallet.getAmount() - walletEvent.getAmount());
                    else {
                        throw new WalletException(ErrorCode.INSUFFICIENT_BALANCE, wallet.getId());
                    }
                }
                walletsToSave.add(wallet);
                eventsToSave.add(walletEvent);
            }
            catch(WalletException e) {
                walletEvent.getFuture().completeExceptionally(e);
            }
        }

        try {
            walletRepository.saveAll(walletsToSave);
            eventsToSave.forEach(walletEvent -> walletEvent.getFuture().complete(null));
        } catch (Exception e) {
            eventsToSave.forEach(walletEvent -> walletEvent.getFuture().completeExceptionally(e));
        }
    }
}
