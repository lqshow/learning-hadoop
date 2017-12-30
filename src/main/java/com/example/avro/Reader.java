package com.example.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Reader {
    public static void main(String[] args) throws Exception {

        // Parser to read our schema definition and create a  Schema object
        Schema schema = new Schema.Parser().parse(new File("src/main/resources/file/user.avsc"));

        // Create Avro file reader
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

        String localSrc = "src/main/resources/file/users.avro";
        String hdfsDst = "input/users.avro";

        // Reading a local file
        readLocalFile(datumReader, localSrc);

        // Reading a hdfs file
        readHdfsFile(schema, hdfsDst);
    }

    /**
     * Reading a local file
     * @param reader
     * @param localSrc
     */
    static void readLocalFile(GenericDatumReader<GenericRecord> reader, String localSrc) {
        try (
                FileReader<GenericRecord> fileReader = DataFileReader.openReader(new File(localSrc), reader)
        ) {
            // Get schema
            Schema schema = fileReader.getSchema();
            // Get fields
            List<Schema.Field> fields = schema.getFields();

            while (fileReader.hasNext()) {
                GenericRecord record = fileReader.next();

                List<Object> columnValues = new ArrayList<>();
                for (Schema.Field field : fields) {
                    columnValues.add(record.get(field.name()));
                }
                System.out.println(columnValues);
            }

            /**
             * output
             *
             * [Alyssa, null, [3, 9, 15, 20]]
             * [Ben, red, []]
             */

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    /**
     * Reading a hdfs file
     * @param hdfsDst
     */
    static void readHdfsFile(Schema schema, String hdfsDst) {
        Configuration conf = new Configuration();
        // Create Avro file reader
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        try (
                FsInput input = new FsInput(new Path(hdfsDst), conf);
                DataFileReader<GenericRecord> fileReader = new DataFileReader<>(input, datumReader)
        ) {

            while (fileReader.hasNext()) {
                GenericRecord record = fileReader.next();
                System.out.println(record);
            }

            /**
             * output
             *
             * {"name": "Alyssa", "favorite_color": null, "favorite_numbers": [3, 9, 15, 20]}
             * {"name": "Ben", "favorite_color": "red", "favorite_numbers": []}
             */

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
