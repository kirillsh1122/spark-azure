package com.sparkpractice.dse;

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.opencagedata.jopencage.model.JOpenCageLatLng;
import com.sparkpractice.dse.models.AddressCoords;
import com.sparkpractice.dse.udfs.GeoHashUDF;
import com.sparkpractice.dse.services.GeoCodesHandler;
import com.sparkpractice.dse.utils.SchemaManager;
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

        // Authenticate to Azure
        DefaultAzureCredential credential = new DefaultAzureCredentialBuilder()
            .managedIdentityClientId(System.getenv("CLIENT_ID"))
            .build();

        // Build Azure Key vault client
        SecretClient vaultClient = new SecretClientBuilder()
                .vaultUrl("https://"+System.getenv("KEY_VAULT_NAME")+".vault.azure.net")
                .credential(credential)
                .buildClient();

        // Setup geoCodesHandlerClient
        GeoCodesHandler geoCodesHandlerClient = new GeoCodesHandler(vaultClient.getSecret("open-cage-api-key").getValue());

        // Get storage account name and SPN data
        String storageAccountName = vaultClient.getSecret("storage-account-name").getValue();
        String clientId = vaultClient.getSecret("client-id").getValue();
        String clientSecret = vaultClient.getSecret("client-secret").getValue();
        String tenantId = vaultClient.getSecret("tenant-id").getValue();

        // Build data location paths
        String hotelSourcePath = "abfss://stage@"+storageAccountName+".dfs.core.windows.net/hotel";
        String weatherSourcePath = "abfss://stage@"+storageAccountName+".dfs.core.windows.net/weather";
        String refinedDataPath = "abfss://curated@"+storageAccountName+".dfs.core.windows.net/refined_data";

        // Build Spark Session
        SparkSession spark = SparkSession.builder().master("local[*]").appName("SparkBasics")
                .config("fs.azure.account.auth.type."+storageAccountName+".dfs.core.windows.net", "OAuth")
                .config("fs.azure.account.oauth.provider.type."+storageAccountName+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
                .config("fs.azure.account.oauth2.client.id."+storageAccountName+".dfs.core.windows.net", clientId)
                .config("fs.azure.account.oauth2.client.secret."+storageAccountName+".dfs.core.windows.net", clientSecret)
                .config("fs.azure.account.oauth2.client.endpoint."+storageAccountName+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantId+"/oauth2/token")
                .getOrCreate();

        // Register UDF
        spark.udf().register("geohash", new GeoHashUDF(), DataTypes.StringType);

        StructType hotelSchema = null;
        DataFrameReader HotelCSVDataFrameReader = null;

        hotelSchema = SchemaManager.getSchemaFromFile("schemas/hotel.json");

        DataFrameReader CSVDataFrameReader = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", "true");

        HotelCSVDataFrameReader = CSVDataFrameReader;
        if (hotelSchema != null)
            HotelCSVDataFrameReader = HotelCSVDataFrameReader.schema(hotelSchema);

        Dataset<Row> hotelDF = HotelCSVDataFrameReader.load(hotelSourcePath);
        hotelDF = hotelDF.withColumn("AddressConcat", concat_ws(
                ", ",
                col("Name"),
                col("City"),
                col("Country")
        ));

        // Hotel address enrichment + GeoHash
        Dataset<Row> hotelOrphanCoordinatesDF = hotelDF.filter(col("latitude").isNull().or(col("longitude").isNull()));
        List<Row> hotelOrphanCoordinatesRowList = hotelOrphanCoordinatesDF.select(col("AddressConcat")).distinct().collectAsList();
        List<String> hotelOrphanCoordinatesList = hotelOrphanCoordinatesRowList.stream().map(x -> (String) x.getAs("AddressConcat")).collect(Collectors.toList());
        Map<String, Optional<JOpenCageLatLng>> addressCoordMap = geoCodesHandlerClient.getBatchCoordinatesBasedOnAddressList(hotelOrphanCoordinatesList);
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

        // Get weather dataframe and apply GeoHash
        Dataset<Row> weatherDF = spark.read().format("parquet").load(weatherSourcePath);
        weatherDF = weatherDF.withColumn("geohash",
                call_udf(
                        "geohash",
                        col("lat"),
                        col("lng")
                )
        );

        // Join dataframes
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

        // Encrypt address with AES
        AESEncryptor encryptor = AESEncryptor.builder().key(vaultClient.getSecret("aes-encryption-key").getValue()).build();
        encryptor.setColumnList(new String[]{"name", "address"});
        joinedDF = joinedDF.transform(encryptor::aesEncrypt);

        // Persist refined data in curated container
        joinedDF.write().mode("overwrite").format("parquet").partitionBy("year", "month", "day").save(refinedDataPath);

        // Close Spark Session
        spark.close();
    }
}
