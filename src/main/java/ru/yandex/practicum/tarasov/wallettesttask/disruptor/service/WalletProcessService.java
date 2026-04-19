package ru.yandex.practicum.tarasov.wallettesttask.disruptor.service;

import jakarta.validation.ConstraintViolationException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.BalanceDto;
import ru.yandex.practicum.tarasov.wallettesttask.DTO.OperationResponseDto;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.event.WalletEvent;
import ru.yandex.practicum.tarasov.wallettesttask.enums.ErrorCode;
import ru.yandex.practicum.tarasov.wallettesttask.enums.OperationResults;
import ru.yandex.practicum.tarasov.wallettesttask.enums.Operations;
import ru.yandex.practicum.tarasov.wallettesttask.service.IdempotencyService;
import ru.yandex.practicum.tarasov.wallettesttask.service.RedisBalanceCheckService;
import ru.yandex.practicum.tarasov.wallettesttask.utility.WalletException;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class WalletProcessService {
    private final TransactionTemplate transactionTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final RedisBalanceCheckService redisBalanceCheckService;
    private final IdempotencyService idempotencyService;

    public WalletProcessService(PlatformTransactionManager transactionManager,
                                JdbcTemplate jdbcTemplate,
                                NamedParameterJdbcTemplate namedParameterJdbcTemplate,
                                RedisBalanceCheckService redisBalanceCheckService,
                                IdempotencyService idempotencyService) {
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.jdbcTemplate = jdbcTemplate;
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
        this.redisBalanceCheckService = redisBalanceCheckService;
        this.idempotencyService = idempotencyService;
    }

    public void processWalletEvents(List<WalletEvent> walletEvents) {

        String updateSql = "UPDATE wallets SET amount = amount + ? WHERE id = ?";
        String selectSql = "SELECT id, amount, currency_code FROM wallets WHERE id in (:ids)";

        HashMap<String, OperationResponseDto> operationResponses = new HashMap<>();
        HashSet<BalanceDto> balanceResponses = new HashSet<>();

        try {
            transactionTemplate.executeWithoutResult((status) ->
                    {
                        int[] results = jdbcTemplate.batchUpdate(updateSql, new BatchPreparedStatementSetter() {
                            @Override
                            public void setValues(PreparedStatement ps, int i) throws SQLException {
                                WalletEvent event = walletEvents.get(i);
                                BigDecimal change = event.getOperation().equals(Operations.DEPOSIT) ?
                                        event.getAmount() : event.getAmount().negate();

                                ps.setBigDecimal(1, change);
                                ps.setObject(2, event.getWalletId());
                            }

                            @Override
                            public int getBatchSize() {
                                return walletEvents.size();
                            }
                        });

                        HashSet<UUID> ids = new HashSet<>();

                        for (int i = 0; i < results.length; i++) {
                            if(results[i] == 0) {
                                idempotencyService.addKey(walletEvents.get(i).getIdempotencyKey(), new OperationResponseDto(walletEvents.get(i).getWalletId(), null, null, OperationResults.FAILED));
                                walletEvents.get(i).getFuture().completeExceptionally(
                                        new WalletException(ErrorCode.WALLET_NOT_FOUND, walletEvents.get(i).getWalletId())
                                );
                            } else {
                                ids.add(walletEvents.get(i).getWalletId());
                            }
                        }

                        Map<String, Object> params = Collections.singletonMap("ids", ids);

                        var queryResults = namedParameterJdbcTemplate.queryForList(selectSql, params);

                        for(var result : queryResults) {
                            OperationResponseDto operationResponseDto = new OperationResponseDto(
                                    (UUID) result.get("id"),
                                    (BigDecimal) result.get("amount"),
                                    (String) result.get("currency_code"),
                                    OperationResults.SUCCESS);

                            BalanceDto balanceDto = new BalanceDto(
                                    (UUID) result.get("id"),
                                    (BigDecimal) result.get("amount"),
                                    (String) result.get("currency_code")
                            );

                            walletEvents
                                    .stream()
                                    .filter(e -> e.getWalletId().equals(result.get("id")))
                                    .forEach(e-> operationResponses.put(e.getIdempotencyKey(), operationResponseDto));
                                    //.forEach(e -> e.getFuture().complete(operationResponseDto));

                            balanceResponses.add(balanceDto);
                        }
                    }
            );
            idempotencyService.addKeys(operationResponses);
            redisBalanceCheckService.updateBalances(balanceResponses);

            walletEvents.forEach(walletEvent -> {
                OperationResponseDto resp = operationResponses.get(walletEvent.getIdempotencyKey());
                if(resp != null) {
                    walletEvent.getFuture().complete(resp);
                } else {
                    idempotencyService.addKey(walletEvent.getIdempotencyKey(), new OperationResponseDto(walletEvent.getWalletId(), null, null, OperationResults.FAILED));
                    walletEvent.getFuture().completeExceptionally(
                            new RuntimeException("No response generated for event")
                    );
                }
            });

        } catch (DataIntegrityViolationException e) {
            processStream(updateSql, walletEvents);
        }
        catch (Exception e) {
            var failedEvents = walletEvents.stream().collect(Collectors.toMap(WalletEvent::getIdempotencyKey, event -> new OperationResponseDto(event.getWalletId(), null, null, OperationResults.FAILED)));
            idempotencyService.addKeys(failedEvents);
            walletEvents.forEach(walletEvent -> walletEvent.getFuture().completeExceptionally(e));
        }
    }

    private void processStream(String updateSql, List<WalletEvent> walletEvents) {
        String selectSql = "SELECT id, amount, currency_code FROM wallets WHERE id = ?";
        for (WalletEvent event : walletEvents) {
            try {
                transactionTemplate.executeWithoutResult((status) -> {
                    BigDecimal change = event.getOperation().equals(Operations.DEPOSIT) ?
                            event.getAmount() : event.getAmount().negate();
                    int result = jdbcTemplate.update(updateSql,  change, event.getWalletId());

                    if(result == 0) {
                        idempotencyService.addKey(event.getIdempotencyKey(), new OperationResponseDto(event.getWalletId(), null, null, OperationResults.FAILED));
                        event.getFuture().completeExceptionally(
                                new WalletException(ErrorCode.WALLET_NOT_FOUND, event.getWalletId())
                        );
                    } else {

                        RowMapper<OperationResponseDto> mapper = (rs, rowNum) -> new OperationResponseDto(
                                rs.getObject("id", UUID.class),
                                rs.getBigDecimal("amount"),
                                rs.getString("currency_code"),
                                OperationResults.SUCCESS
                        );

                        List<OperationResponseDto> queryResults = jdbcTemplate.query(selectSql, mapper, event.getWalletId());
                        OperationResponseDto operationResponseDto = queryResults.stream().findFirst().orElseThrow(() -> new WalletException(ErrorCode.WALLET_NOT_FOUND));

                        idempotencyService.addKey(event.getIdempotencyKey(), operationResponseDto);
                        redisBalanceCheckService.updateBalance(
                                new BalanceDto(
                                        operationResponseDto.walletId(),
                                        operationResponseDto.amount(),
                                        operationResponseDto.currencyCode()
                                )
                        );

                        event.getFuture().complete(operationResponseDto);
                    }
                });
            }
            catch (DataIntegrityViolationException e) {
                idempotencyService.addKey(event.getIdempotencyKey(), new OperationResponseDto(event.getWalletId(), null, null, OperationResults.FAILED));
                event.getFuture().completeExceptionally(
                        new WalletException(ErrorCode.INSUFFICIENT_BALANCE, event.getWalletId())
                );
            }
            catch (Exception e) {
                idempotencyService.addKey(event.getIdempotencyKey(), new OperationResponseDto(event.getWalletId(), null, null, OperationResults.FAILED));
                event.getFuture().completeExceptionally(e);
            }
        }
    }
}
