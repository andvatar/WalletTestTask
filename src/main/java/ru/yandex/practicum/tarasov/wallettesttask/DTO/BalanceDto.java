package ru.yandex.practicum.tarasov.wallettesttask.DTO;

import java.util.UUID;

public record BalanceDto(UUID id, long amount, String currencyCode) {
}
