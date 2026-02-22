package com.sparkpractice.dse.models.mappers;

import com.sparkpractice.dse.models.AddressCoords;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class AddressCoordMapper implements MapFunction<Row, AddressCoords> {

    @Override
    public AddressCoords call(Row row) throws Exception {
        return null;
    }
}
