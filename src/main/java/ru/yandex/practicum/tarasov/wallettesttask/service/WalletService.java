package ru.yandex.practicum.tarasov.wallettesttask.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.BalanceDto;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.BalanceMapper;
import ru.yandex.practicum.tarasov.wallettesttask.enums.ErrorCode;
import ru.yandex.practicum.tarasov.wallettesttask.repository.WalletRepository;
import ru.yandex.practicum.tarasov.wallettesttask.units.WalletException;

import java.util.UUID;

@Service
public class WalletService {

    public WalletService(WalletRepository walletRepository,  BalanceMapper balanceMapper) {
        this.walletRepository = walletRepository;
        this.mapper = balanceMapper;
    }

    private final BalanceMapper mapper;
    private final WalletRepository walletRepository;

    public BalanceDto getBalance(UUID walletId) {
        return mapper.getBalanceDto(walletRepository
                .findById(walletId)
                .orElseThrow(() -> new WalletException(ErrorCode.WALLET_NOT_FOUND, walletId)));
    }


}
