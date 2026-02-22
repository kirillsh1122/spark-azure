package com.sparkpractice.dse.utils;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;


public class Utils {

    public static StructType getSchema(String pathToSchema) throws IOException {
        String schemaString = new String(Files.readAllBytes(Paths.get(pathToSchema)));
        return (StructType) DataType.fromJson(schemaString);
    }
}
