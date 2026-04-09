package ru.yandex.practicum.tarasov.wallettesttask.disruptor.handler;

import com.lmax.disruptor.EventHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.event.WalletEvent;
import ru.yandex.practicum.tarasov.wallettesttask.enums.ErrorCode;
import ru.yandex.practicum.tarasov.wallettesttask.enums.Operations;
import ru.yandex.practicum.tarasov.wallettesttask.units.WalletException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

@Getter
@Slf4j
public class WalletEventProcessor implements EventHandler<WalletEvent> {
    private final List<WalletEvent> walletEvents;
    private final int workerNumber;
    private final TransactionTemplate transactionTemplate;
    private final JdbcTemplate jdbcTemplate;

    public WalletEventProcessor(int workerNumber,
                                PlatformTransactionManager transactionManager,
                                JdbcTemplate jdbcTemplate) {
        this.walletEvents = new ArrayList<>();
        this.workerNumber = workerNumber;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void onEvent(WalletEvent event, long sequence, boolean endOfBatch) {
        walletEvents.add(event);

        if(endOfBatch) {
            try {
                log.info("bufferSize: {}", walletEvents.size());
                processWalletEvents(walletEvents);
            }
            finally {
                walletEvents.forEach(WalletEvent::clear);
                walletEvents.clear();
            }
        }
    }

    private void processWalletEvents(List<WalletEvent> walletEvents) {

        String sql = "UPDATE wallets SET amount = amount + ? WHERE id = ?";
        try {
            transactionTemplate.executeWithoutResult((status) ->
                    {
                        int[] results = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                            @Override
                            public void setValues(PreparedStatement ps, int i) throws SQLException {
                                WalletEvent event = walletEvents.get(i);
                                long change = event.getOperation().equals(Operations.DEPOSIT) ?
                                        event.getAmount() : -event.getAmount();

                                ps.setLong(1, change);
                                ps.setObject(2, event.getWalletId());
                            }

                            @Override
                            public int getBatchSize() {
                                return walletEvents.size();
                            }
                        });
                        for (int i = 0; i < results.length; i++) {
                            if(results[i] == 0) {
                                walletEvents.get(i).getFuture().completeExceptionally(
                                        new WalletException(ErrorCode.WALLET_NOT_FOUND, walletEvents.get(i).getWalletId())
                                );
                            } else {
                                walletEvents.get(i).getFuture().complete(null);
                            }
                        }
                    }
            );
        } catch (DataAccessException e) {
            processStream(sql, walletEvents);
        }
        catch (Exception e) {
            walletEvents.forEach(walletEvent -> walletEvent.getFuture().completeExceptionally(e));
        }
    }

    private void processStream(String sql, List<WalletEvent> walletEvents) {
        for (WalletEvent event : walletEvents) {
            try {
                transactionTemplate.executeWithoutResult((status) -> {
                    long change = event.getOperation().equals(Operations.DEPOSIT) ?
                            event.getAmount() : -event.getAmount();
                    int result = jdbcTemplate.update(sql,  change, event.getWalletId());

                    if(result == 0) {
                        event.getFuture().completeExceptionally(
                                new WalletException(ErrorCode.WALLET_NOT_FOUND, event.getWalletId())
                        );
                    } else {
                        event.getFuture().complete(null);
                    }
                });
            }
            catch (DataAccessException e) {
                event.getFuture().completeExceptionally(
                        new WalletException(ErrorCode.INSUFFICIENT_BALANCE, event.getWalletId())
                );
            }
        catch (Exception e) {
                event.getFuture().completeExceptionally(e);
            }
        }
    }
}
