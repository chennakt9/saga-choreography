package com.chenna.orderms.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
public class OrderTable {
    @Id
    @GeneratedValue
    private long id;

    @Column
    private String item;

    @Column
    private int quantity;

    @Column
    private double amount;

    @Column
    private String status;
}
