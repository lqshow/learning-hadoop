package com.example.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

public class Writer {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        // First, we use a Parser to read our schema definition and create a Schema object.
        Schema schema = new Schema.Parser().parse(new File("src/main/resources/file/user.avsc"));

        // Create a DatumWriter
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);

        try (
                FileSystem fs = FileSystem.get(conf);
                DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter)
        ) {
            // Using this schema, let's create some users.
            GenericRecord user1 = new GenericData.Record(schema);
            user1.put("name", "Alyssa");
            user1.put("favorite_numbers", Arrays.asList(new Integer[]{256}));
            // Leave favorite color null

            GenericRecord user2 = new GenericData.Record(schema);
            user2.put("name", "Ben");
            user2.put("favorite_numbers", Arrays.asList(new Integer[]{7}));
            user2.put("favorite_color", "red");

            OutputStream os = fs.create(new Path("output/users.avro"));

            dataFileWriter.create(schema, os);

            dataFileWriter.append(user1);
            dataFileWriter.append(user2);
        }
    }
}
