package com.sparkpractice.dse;

import com.opencagedata.jopencage.model.JOpenCageLatLng;
import com.sparkpractice.dse.models.AddressCoords;
import com.sparkpractice.dse.udfs.GeoHashUDF;
import com.sparkpractice.dse.services.GeoCodesHandler;
import com.sparkpractice.dse.utils.Utils;
import com.sparkpractice.dse.services.AESEncryptor;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;


public class App {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkBasics")
                .getOrCreate();

        spark.udf().register("geohash", new GeoHashUDF(), DataTypes.StringType);

        StructType hotelSchema = null;
        DataFrameReader HotelCSVDataFrameReader = null;

        try {
            hotelSchema = Utils.getSchema("src/main/resources/schemas/hotel.json");
        } catch (IOException e) {
            System.out.println("caught exception reading the schema");
            System.out.println(e.getMessage());
        }

        DataFrameReader CSVDataFrameReader = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", "true");

        HotelCSVDataFrameReader = CSVDataFrameReader;
        if (hotelSchema != null)
            HotelCSVDataFrameReader = HotelCSVDataFrameReader.schema(hotelSchema);

        Dataset<Row> hotelDF = HotelCSVDataFrameReader.load("/mnt/sharedfolder1/m06sparkbasics/hotels");
        hotelDF = hotelDF.withColumn("AddressConcat", concat_ws(
                ", ",
                col("Name"),
                col("City"),
                col("Country")
        ));

        Dataset<Row> hotelOrphanCoordinatesDF = hotelDF.filter(col("latitude").isNull().or(col("longitude").isNull()));
        List<Row> hotelOrphanCoordinatesRowList = hotelOrphanCoordinatesDF.select(col("AddressConcat")).distinct().collectAsList();
        List<String> hotelOrphanCoordinatesList = hotelOrphanCoordinatesRowList.stream().map(x -> (String) x.getAs("AddressConcat")).collect(Collectors.toList());
        Map<String, Optional<JOpenCageLatLng>> addressCoordMap = GeoCodesHandler.getBatchCoordinatesBasedOnAddressList(hotelOrphanCoordinatesList);
        List<AddressCoords> AddressCoordsList = new ArrayList<>();
        addressCoordMap.forEach((k, v) -> {
            Double latitude = v.isPresent() ? v.get().getLat(): Double.NaN;
            Double longitude = v.isPresent() ? v.get().getLng(): Double.NaN;
            AddressCoordsList.add(new AddressCoords(k, longitude, latitude));
        });
        Dataset<Row> AddressCoordsDF = spark.createDataset(AddressCoordsList, Encoders.bean(AddressCoords.class)).toDF();

        hotelDF = hotelDF.alias("hotel").join(
                AddressCoordsDF.alias("ref"),
                col("hotel.AddressConcat").equalTo(col("ref.AddressConcat")),
                "left_outer"
        ).select(
                col("hotel.id"),
                col("hotel.name"),
                col("hotel.country"),
                col("hotel.city"),
                col("hotel.address"),
                coalesce(col("hotel.latitude"), col("ref.latitude")).alias("latitude"),
                coalesce(col("hotel.longitude"), col("ref.longitude")).alias("longitude")
        ).withColumn("geohash",
                call_udf(
                        "geohash",
                        col("latitude"),
                        col("longitude")
                )
        );
        hotelDF.show(10, false);

        Dataset<Row> weatherDF = spark.read().format("parquet").load("/mnt/sharedfolder1/m06sparkbasics/weather");
        weatherDF = weatherDF.withColumn("geohash",
                call_udf(
                        "geohash",
                        col("lat"),
                        col("lng")
                )
        );

        Dataset<Row> joinedDF = weatherDF.alias("w").join(
                hotelDF.alias("h"),
                col("h.geohash").equalTo(col("w.geohash")),
                "inner"
        ).select(
                col("h.id"),
                col("h.name"),
                col("h.country"),
                col("h.city"),
                col("h.address"),
                col("h.latitude"),
                col("h.longitude"),
                col("h.geohash"),
                col("w.avg_tmpr_f"),
                col("w.avg_tmpr_c"),
                col("w.wthr_date"),
                col("w.year"),
                col("w.month"),
                col("w.day")
        );

        AESEncryptor encryptor = new AESEncryptor(new String[]{"name", "address"});
        joinedDF = joinedDF.transform(encryptor::aesEncrypt);

        joinedDF.write().format("parquet").partitionBy("year", "month", "day").save("/mnt/sharedfolder1/m06sparkbasics/refined_data");

        spark.close();
    }
}
