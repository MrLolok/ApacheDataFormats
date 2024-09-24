package org.synclab.avro;

import lombok.RequiredArgsConstructor;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.synclab.avro.data.Application;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

@RequiredArgsConstructor
public class AvroDataHandler<T> {
    private final File file;

    public void write(List<T> dataset, Class<T> clazz) {
        DatumWriter<T> applicationDatumWriter = new SpecificDatumWriter<>(clazz);
        try (DataFileWriter<T> dataFileWriter = new DataFileWriter<>(applicationDatumWriter)) {
            dataFileWriter.create(Application.getClassSchema(), file);
            for (T record : dataset)
                dataFileWriter.append(record);
        } catch (IOException e) {
            throw new RuntimeException("Errore durante la creazione del DataFileWriter", e);
        }
    }

    public void read(@Nullable Consumer<T> consumer, Class<T> clazz) {
        DatumReader<T> datumReader = new SpecificDatumReader<>(clazz);
        try (DataFileReader<T> dataFileReader = new DataFileReader<>(file, datumReader)) {
            T record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                if (consumer != null)
                    consumer.accept(record);
            }
        } catch (IOException e) {
            throw new RuntimeException("Errore durante la creazione del DataFileReader", e);
        }
    }
}
