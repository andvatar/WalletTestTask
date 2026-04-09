package ru.yandex.practicum.tarasov.wallettesttask.utility;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.MessageSource;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.ErrorDto;
import ru.yandex.practicum.tarasov.wallettesttask.enums.ErrorCode;

import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.CompletionException;

@ControllerAdvice
@AllArgsConstructor
@Slf4j
public class WalletGlobalExceptionHandler {

    private final MessageSource messageSource;

    @ExceptionHandler(WalletException.class)
    public ResponseEntity<ErrorDto> handleWalletExceptions(WalletException exception, Locale locale) {
        log.warn(exception.getMessage(), exception);
        return createResponseEntity(exception.getErrorCode(), exception.getArgs(), locale);
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, HttpMessageNotReadableException.class})
    public ResponseEntity<ErrorDto> handleValidationExceptions(Exception exception,  Locale locale) {
        log.error(exception.getMessage(), exception);
        return createResponseEntity(ErrorCode.INVALID_FORMAT, null, locale);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorDto> handleOtherExceptions(Exception exception, Locale locale) {
        log.error(exception.getMessage(), exception);
        return createResponseEntity(ErrorCode.INTERNAL_SERVER_ERROR, null, locale);
    }

    @ExceptionHandler(CompletionException.class)
    public ResponseEntity<ErrorDto> handleCompletionException(Exception exception, Locale locale) {
        Throwable cause = exception.getCause();

        if(cause instanceof WalletException walletException) {
            return handleWalletExceptions(walletException, locale);
        }
        if(cause instanceof Exception ex) {
            return handleOtherExceptions(ex, locale);
        }
        return createResponseEntity(ErrorCode.INTERNAL_SERVER_ERROR, null, locale);
    }

    private ResponseEntity<ErrorDto> createResponseEntity(ErrorCode errorCode, Object[] args, Locale locale) {
        String errorMessage = messageSource.getMessage(errorCode.getMessageCode(), null, locale);

        return ResponseEntity
                .status(errorCode.getHttpStatus())
                .body(new ErrorDto(
                        Instant.now(),
                        errorCode.getMessageCode(),
                        String.format(errorMessage, args))
                );
    }
}
