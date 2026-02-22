package com.sparkpractice.dse.services;


import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

@RequiredArgsConstructor
public class AESEncryptor {

    private final String[] columnList;

    private static final String key;

    static {
        if ((key = System.getenv("AES_ENCRYPT_KEY")) == null) {
            throw new RuntimeException("AES_ENCRYPT_KEY is not provided");
        }
    }

    public Dataset<Row> aesEncrypt(Dataset<Row> inputDf) {
        for (String attribute: columnList) {
            inputDf = inputDf.withColumn(
                    attribute,
                    base64(aes_encrypt(col(attribute), lit(key), lit("GCM"), lit("DEFAULT")))
            );
        }
        return inputDf;
    }
}
