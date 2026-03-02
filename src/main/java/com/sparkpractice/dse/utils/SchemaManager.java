package com.sparkpractice.dse.utils;


import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataType;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class SchemaManager {

    public static StructType getSchemaFromFile(String resourcePath) {

        try (
                InputStream is = Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream(resourcePath)) {

            if (is == null) {
                throw new IllegalArgumentException("Schema not found in resources: " + resourcePath);
            }

            String schemaString = new String(is.readAllBytes(), StandardCharsets.UTF_8);

            return (StructType) DataType.fromJson(schemaString);

        } catch (Exception e) {
            throw new RuntimeException("Failed to load schema: " + resourcePath, e);
        }
    }
}
