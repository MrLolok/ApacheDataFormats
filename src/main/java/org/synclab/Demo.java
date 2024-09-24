package org.synclab;

import org.synclab.avro.AvroDataHandler;
import org.synclab.avro.data.Application;
import org.synclab.csv.ApplicationCSVReader;
import org.synclab.parquet.ParquetDataHandler;

import java.io.IOException;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

public class Demo {
    private final static Path APPLICATIONS_FILE_AVRO = Path.of("data", "applications.avro");
    private final static Path APPLICATIONS_FILE_CSV = Path.of("src", "main", "resources", "applications.csv");
    private final static org.apache.hadoop.fs.Path APPLICATIONS_FILE_PARQUET = new org.apache.hadoop.fs.Path("data/applications.parquet");

    public static void main(String[] args) {
        ApplicationCSVReader applicationCSVReader = new ApplicationCSVReader();
        List<Application> applications = applicationCSVReader.read(APPLICATIONS_FILE_CSV.toFile(), true).stream().map(row -> {
            try {
                return applicationCSVReader.parse(row);
            } catch (ParseException e) {
                Logger.getLogger("Application Parser", "Impossibile leggere l'entry " + row);
            }
            return null;
        }).filter(Objects::nonNull).toList();

        // Avro
        AvroDataHandler<Application> avroDataHandler = new AvroDataHandler<>(APPLICATIONS_FILE_AVRO.toFile());
        avroDataHandler.write(applications, Application.class);
        avroDataHandler.read(System.out::println, Application.class);

        // Parquet
        ParquetDataHandler<Application> parquetDataHandler = new ParquetDataHandler<>();
        try {
            parquetDataHandler.write(applications, APPLICATIONS_FILE_PARQUET, Application.getClassSchema());
            parquetDataHandler.read(System.out::println, APPLICATIONS_FILE_PARQUET);
        } catch (IOException exception) {
            Logger.getLogger("Applications Parquet", exception.getMessage());
        }
    }
}
