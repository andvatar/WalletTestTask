package ru.yandex.practicum.tarasov.wallettesttask.DTO;

import org.mapstruct.Mapper;
import ru.yandex.practicum.tarasov.wallettesttask.entity.Wallet;

@Mapper(componentModel = "spring")
public interface BalanceMapper {
    BalanceDto getBalanceDto(Wallet wallet);
}
