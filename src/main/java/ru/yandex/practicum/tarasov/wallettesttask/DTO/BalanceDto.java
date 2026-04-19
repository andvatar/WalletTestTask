package ru.yandex.practicum.tarasov.wallettesttask.DTO;

import java.math.BigDecimal;
import java.util.UUID;

public record BalanceDto(UUID id, BigDecimal amount, String currencyCode) {
}
