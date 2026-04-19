package ru.yandex.practicum.tarasov.wallettesttask.DTO;

import jakarta.annotation.Nullable;
import ru.yandex.practicum.tarasov.wallettesttask.enums.OperationResults;

import java.math.BigDecimal;
import java.util.UUID;

public record OperationResponseDto(UUID walletId,
                                   @Nullable BigDecimal amount,
                                   @Nullable String currencyCode,
                                   OperationResults result) {
}
