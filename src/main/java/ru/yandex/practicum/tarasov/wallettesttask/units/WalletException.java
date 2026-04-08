package ru.yandex.practicum.tarasov.wallettesttask.units;

import lombok.Getter;
import ru.yandex.practicum.tarasov.wallettesttask.enums.ErrorCode;

@Getter
public class WalletException extends RuntimeException {
    private final ErrorCode errorCode;
    private final Object[] args;

    public WalletException(ErrorCode errorCode, Object... args) {
        super(errorCode.name());
        this.errorCode = errorCode;
        this.args = args;
    }
}
