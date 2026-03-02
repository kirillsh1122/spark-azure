package com.sparkpractice.dse.models;


import lombok.Data;

@Data
public class AddressCoords {
    private final String addressConcat;
    private final Double longitude;
    private final Double latitude;
}
