package ru.yandex.practicum.tarasov.wallettesttask.DTO;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import ru.yandex.practicum.tarasov.wallettesttask.enums.Operations;

import java.util.UUID;

public record OperationDto(
        @NotNull(message = "Wallet ID must not be empty")
        UUID walletId,
        @NotNull(message = "Operation type must not be empty")
        Operations operationType,
        @Min(value = 0, message = "Operation amount must be positive")
        long amount
) {

}
