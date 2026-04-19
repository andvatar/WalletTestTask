package ru.yandex.practicum.tarasov.wallettesttask.disruptor.config;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.event.WalletEvent;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.event.WalletEventFactory;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.handler.WalletEventProcessor;
import ru.yandex.practicum.tarasov.wallettesttask.disruptor.service.WalletProcessService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@Configuration
@Slf4j
public class DisruptorConfig {
    @Value("${app.shards}")
    private int shardsNumber;

    @Value("${app.buffer_size}")
    private int bufferSize;
    private final WalletProcessService walletProcessService;
    private final List<Disruptor<WalletEvent>> disruptors;

    public DisruptorConfig(WalletProcessService walletProcessService) {
        this.walletProcessService = walletProcessService;
        disruptors =  new ArrayList<>();
    }

    @Bean("ringBuffers")
    public List<RingBuffer<WalletEvent>> walletEventRingBuffers() {
        List<RingBuffer<WalletEvent>> ringBuffers = new ArrayList<>();

        for (int i = 0; i < shardsNumber; i++) {
            ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
            WalletEventFactory factory = new WalletEventFactory();
            Disruptor<WalletEvent> disruptor = new Disruptor<>(factory, bufferSize, threadFactory, ProducerType.MULTI, new BlockingWaitStrategy());
            disruptor.handleEventsWith(new WalletEventProcessor(i, walletProcessService));
            disruptor.start();
            disruptors.add(disruptor);
            RingBuffer<WalletEvent> ringBuffer = disruptor.getRingBuffer();
            ringBuffers.add(ringBuffer);
        }

        return ringBuffers;
    }

    @PreDestroy
    private void disruptorShutdown() {
        for (Disruptor<WalletEvent> disruptor : disruptors) {
            try {
                disruptor.shutdown(10, TimeUnit.SECONDS);
                log.info("Disruptor shut down successfully");
            } catch (Exception e) {
                disruptor.halt();
                log.warn("Disruptor shut down failed", e);
            }
        }
    }
}
