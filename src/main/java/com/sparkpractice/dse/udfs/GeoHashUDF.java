package com.sparkpractice.dse.udfs;

import ch.hsr.geohash.GeoHash;
import org.apache.spark.sql.api.java.UDF2;

public class GeoHashUDF implements UDF2<Double, Double, String> {

    @Override
    public String call(Double latitude, Double longitude) throws Exception {
        return GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, 4);
    }
}
