package ru.yandex.practicum.tarasov.wallettesttask.disruptor.config;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.event.WalletEvent;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.event.WalletEventFactory;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.handler.WalletEventProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;

@Configuration
public class DisruptorConfig {
    @Value("${app.shards}")
    private int shardsNumber;

    @Value("${app.buffer_size}")
    private int bufferSize;

    private final PlatformTransactionManager transactionManager;
    private final JdbcTemplate jdbcTemplate;

    public DisruptorConfig(PlatformTransactionManager transactionManager,
                           JdbcTemplate jdbcTemplate) {
        this.transactionManager = transactionManager;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Bean("ringBuffers")
    public List<RingBuffer<WalletEvent>> walletEventRingBuffers() {
        List<RingBuffer<WalletEvent>> ringBuffers = new ArrayList<>();

        for (int i = 0; i < shardsNumber; i++) {
            ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
            WalletEventFactory factory = new WalletEventFactory();
            Disruptor<WalletEvent> disruptor = new Disruptor<>(factory, bufferSize, threadFactory, ProducerType.MULTI, new BlockingWaitStrategy());
            disruptor.handleEventsWith(new WalletEventProcessor(i, transactionManager, jdbcTemplate));
            disruptor.start();
            RingBuffer<WalletEvent> ringBuffer = disruptor.getRingBuffer();
            ringBuffers.add(ringBuffer);
        }

        return ringBuffers;
    }
}
