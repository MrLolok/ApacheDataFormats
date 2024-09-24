package org.synclab.parquet;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

public class ParquetDataHandler<T> {
    public void write(List<T> dataset, Path path, Schema schema) throws IOException {
        OutputFile outputFile = HadoopOutputFile.fromPath(path, new Configuration());
        ParquetWriter<T> writer = AvroParquetWriter.<T>builder(outputFile)
                .withSchema(schema)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false")
                .build();
        for (T record : dataset)
            writer.write(record);
        writer.close();
    }

    public void read(Consumer<T> consumer, Path path) throws IOException {
        InputFile inputFile = HadoopInputFile.fromPath(path, new Configuration());
        try (ParquetReader<T> reader = AvroParquetReader
                .<T>builder(inputFile)
                .withConf(new Configuration())
                .build()) {

            T record;
            while ((record = reader.read()) != null) {
                consumer.accept(record);
            }
        }
    }
}
