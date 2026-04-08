package ru.yandex.practicum.tarasov.wallettesttask.DTO;

import java.time.Instant;

public record ErrorDto(Instant timestamp, String errorCode, String errorMessage) {
}
