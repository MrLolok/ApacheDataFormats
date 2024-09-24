package org.synclab.parquet;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

public class ParquetDataHandler<T> {
    public void write(List<T> dataset, Path path, Schema schema) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path.toUri());
        OutputFile outputFile = HadoopOutputFile.fromPath(hadoopPath, new Configuration());
        ParquetWriter<T> writer = AvroParquetWriter.<T>builder(outputFile)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
        for (T record : dataset)
            writer.write(record);
        writer.close();
    }

    public void read(Consumer<T> consumer, Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path.toUri());
        InputFile inputFile = HadoopInputFile.fromPath(hadoopPath, new Configuration());
        try (ParquetReader<T> reader = AvroParquetReader
                .<T>builder(inputFile)
                .build()) {

            T record;
            while ((record = reader.read()) != null) {
                consumer.accept(record);
            }
        }
    }
}
