package com.sparkpractice.dse.services;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

@Builder
@AllArgsConstructor
public class AESEncryptor {

    @Setter
    @Builder.Default
    private String[] columnList = new String[]{};
    private final String key;

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
