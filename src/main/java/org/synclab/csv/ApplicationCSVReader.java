package org.synclab.csv;

import org.synclab.avro.data.Application;
import org.synclab.avro.data.ApplicationType;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.List;

public class ApplicationCSVReader extends CSVReader<Application> {
    private final static DecimalFormat RATING_DECIMAL_FORMAT = new DecimalFormat("#.#");

    public Application parse(List<String> list) throws ParseException, NumberFormatException {
        return Application.newBuilder()
                .setName(list.getFirst())
                .setCategory(list.get(1))
                .setRating(parseApplicationRating(list.get(2)))
                .setReviews(parseApplicationReviews(list.get(3)))
                .setSizeInKilobytes(parseApplicationSize(list.get(4)))
                .setInstallsApprox(parseApplicationInstalls(list.get(5)))
                .setType(parseApplicationType(list.get(6)))
                .setPrice(parseApplicationPrice(list.get(7)))
                .build();
    }

    private Float parseApplicationRating(String raw) throws ParseException {
        return raw.equals("NaN") ? null : RATING_DECIMAL_FORMAT.parse(raw).floatValue();
    }

    private int parseApplicationReviews(String raw) throws NumberFormatException {
        return Integer.parseInt(raw);
    }

    private Object parseApplicationSize(String raw) {
        Object size = raw;
        try {
            size = (int) (Float.parseFloat(raw.replaceAll("[^\\d.]", "")) * (raw.contains("M") ? 1000 : 1));
        } catch (Exception ignored) {}
        return size;
    }

    private long parseApplicationInstalls(String raw) {
        return Long.parseLong(raw.replaceAll("[^\\d.]", ""));
    }

    private ApplicationType parseApplicationType(String raw) {
        ApplicationType type = ApplicationType.UNKNOWN;
        try {
            type = ApplicationType.valueOf(raw.toUpperCase());
        } catch (IllegalArgumentException ignored) {}
        return type;
    }

    private float parseApplicationPrice(String raw) {
        float price = 0;
        try {
            price = Float.parseFloat(raw.replaceAll("[^\\d.]", ""));
        } catch (NumberFormatException ignored) {}
        return price;
    }
}
