package ru.yandex.practicum.tarasov.wallettesttask.DTO;

import ru.yandex.practicum.tarasov.wallettesttask.enums.Operations;

import java.util.UUID;

public record OperationDto(UUID walletId, Operations operationType, long amount) {

}
