package ru.yandex.practicum.tarasov.wallettesttask.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
@AllArgsConstructor
public enum ErrorCode {
    WALLET_NOT_FOUND("error.wallet.not_found", HttpStatus.NOT_FOUND),
    INSUFFICIENT_BALANCE("error.insufficient_balance", HttpStatus.BAD_REQUEST),
    INVALID_FORMAT("error.invalid_format", HttpStatus.BAD_REQUEST),
    INTERNAL_SERVER_ERROR("error.internal_server_error", HttpStatus.INTERNAL_SERVER_ERROR),
    OPERATION_IN_PROGRESS("error.operation_in_progress", HttpStatus.TOO_EARLY);

    private final String messageCode;
    private final HttpStatus httpStatus;

}
