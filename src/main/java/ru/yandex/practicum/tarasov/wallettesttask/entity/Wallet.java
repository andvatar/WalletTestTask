package ru.yandex.practicum.tarasov.wallettesttask.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.DecimalMin;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "wallets")
@NoArgsConstructor
@Setter
@Getter
public class Wallet {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id")
    private UUID id;
    @DecimalMin(value = "0")
    @Column(name = "amount")
    private long amount;
    @Column(name = "currency_code")
    private String currencyCode;
    @Column(name = "created")
    private Instant creationDate;
    @Column(name = "updated")
    private Instant lastUpdate;
}
