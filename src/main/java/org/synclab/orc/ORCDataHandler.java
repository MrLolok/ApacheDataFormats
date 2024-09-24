package org.synclab.orc;

import org.apache.commons.lang3.function.TriConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.*;
import org.apache.hadoop.hive.ql.exec.vector.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.BiConsumer;

public class ORCDataHandler<T> {
    public void write(List<T> dataset, TriConsumer<VectorizedRowBatch, Integer, T> consumer, Path path, TypeDescription schema) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path.toUri());
        Configuration conf = new Configuration();
        try (Writer writer = OrcFile.createWriter(hadoopPath, OrcFile.writerOptions(conf).setSchema(schema))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            for (T record : dataset) {
                int row = batch.size++;
                consumer.accept(batch, row, record);
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
    }

    public void read(BiConsumer<VectorizedRowBatch, Integer> consumer, Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path.toUri());
        Configuration conf = new Configuration();
        try (Reader reader = OrcFile.createReader(hadoopPath, OrcFile.readerOptions(conf))) {
            try (RecordReader rows = reader.rows()) {
                VectorizedRowBatch batch = reader.getSchema().createRowBatch();
                while (rows.nextBatch(batch))
                    for (int row = 0; row < batch.size; ++row)
                        consumer.accept(batch, row);
            }
        }
    }
}
