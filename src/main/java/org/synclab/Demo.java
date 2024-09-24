package org.synclab;

import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.synclab.avro.AvroDataHandler;
import org.synclab.avro.data.Application;
import org.synclab.csv.ApplicationCSVReader;
import org.synclab.orc.ORCDataHandler;
import org.synclab.parquet.ParquetDataHandler;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public class Demo {
    private static final Logger LOGGER = LoggerFactory.getLogger(Demo.class);

    private final static Path APPLICATIONS_FILE_CSV = Path.of("src", "main", "resources", "applications.csv");
    private final static Path APPLICATIONS_DATA_FOLDER = Path.of("data");
    private final static Path APPLICATIONS_FILE_AVRO = Path.of(APPLICATIONS_DATA_FOLDER.toString(), "applications.avro");
    private final static Path APPLICATIONS_FILE_PARQUET = Path.of(APPLICATIONS_DATA_FOLDER.toString(), "applications.parquet");
    private final static Path APPLICATIONS_FILE_ORC = Path.of(APPLICATIONS_DATA_FOLDER.toString(), "applications.orc");

    public static void main(String[] args) {
        if (APPLICATIONS_DATA_FOLDER.toFile().mkdirs())
            LOGGER.info("Creata cartella per salvare i file in formato Apache.");

        ApplicationCSVReader applicationCSVReader = new ApplicationCSVReader();
        List<Application> applications = applicationCSVReader.read(APPLICATIONS_FILE_CSV.toFile(), true).stream().map(row -> {
            try {
                return applicationCSVReader.parse(row);
            } catch (Exception e) {
                e.printStackTrace();
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
            LOGGER.info(exception.getMessage());
        }

        // ORC
        ORCDataHandler<Application> orcDataHandler = new ORCDataHandler<>();
        TypeDescription schema = TypeDescription.fromString("struct<name:string,category:string,rating:float,reviews:int,size_in_kilobytes:int,type:int,price:float>");
        try {
            orcDataHandler.write(applications, null, APPLICATIONS_FILE_ORC, schema);
            orcDataHandler.read(null, APPLICATIONS_FILE_ORC);
        } catch (IOException exception) {
            LOGGER.info(exception.getMessage());
        }
    }
}
