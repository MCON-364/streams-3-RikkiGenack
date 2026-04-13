package edu.touro.las.mcon364.streams.ds;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.stream.*;


public class WeatherDataScienceExercise {

    record WeatherRecord(
            String stationId,
            String city,
            String date,
            double temperatureC,
            int humidity,
            double precipitationMm
    ) {}

    public static void main(String[] args) throws Exception {
        List<String> rows = readCsvRows("noaa_weather_sample_200_rows.csv");

        List<WeatherRecord> cleaned = rows.stream()
                .skip(1) // skip header
                .map(WeatherDataScienceExercise::parseRow)
                .flatMap(Optional::stream)
                .filter(WeatherDataScienceExercise::isValid)
                .toList();

        System.out.println("Total raw rows (excluding header): " + (rows.size() - 1));
        System.out.println("Total cleaned rows: " + cleaned.size());

        // TODO 1:
        // Count how many valid weather records remain after cleaning.

        // TODO 2:
        // Compute the average temperature across all valid rows.

        // TODO 3:
        // Find the city with the highest average temperature.

        // TODO 4:
        // Group records by city.

        // TODO 5:
        // Compute average precipitation by city.
        cleaned.stream().collect(Collectors.groupingBy(WeatherRecord::city,  Collectors.averagingDouble(WeatherRecord::temperatureC)));

        // TODO 6:
        // Partition rows into freezing days (temperature <= 0)
        // and non-freezing days (temperature > 0).

        // TODO 7:
        // Create a Set<String> of all distinct cities.

        // TODO 8:
        // Find the wettest single day.
        //cleaned.stream().map(WeatherRecord::precipitationMm).sorted().
        // TODO 9:
        // Create a Map<String, Double> from city to average humidity.

        // TODO 10:
        // Produce a list of formatted strings like:
        // "Miami on 2025-01-02: 25.1C, humidity 82%"
        // TODO 11 (optional):
        // Build a Map<String, CityWeatherSummary> for all cities.

        // Put your code below these comments or refactor into helper methods.
    }

    static Optional<WeatherRecord> parseRow(String row) {
        // TODO:
        // 1. Split the row by commas
        // 2. Reject malformed rows
        // 3. Reject rows with missing temperature
        // 4. Parse numeric values safely
        // 5. Return Optional.empty() if parsing fails
        if (row ==null || row.isEmpty()) {
            return Optional.empty();
        }
        String[] splitted = row.split(", ");
        if (splitted.length != 6) {
            return Optional.empty();
        }
        try {
            String stationId = splitted[0];
            String city = splitted[1];
            String date = splitted[2];
            double temperatureC = Double.parseDouble(splitted[3]);
            int humidity = Integer.parseInt(splitted[4]);
            double precipitationMm = Double.parseDouble(splitted[5]);
            return Optional.of(new WeatherRecord
                    (stationId, city, date, temperatureC, humidity, precipitationMm));
        } catch(NumberFormatException e) {
            return Optional.empty();
        }

    }

    static boolean isValid(WeatherRecord r) {
        // TODO:
        // Keep only rows where:
        // - temperature is between -60 and 60
        // - humidity is between 0 and 100
        // - precipitation is >= 0
        if (r.temperatureC>-59 && r.temperatureC<61 && r.humidity>0 && r.humidity <100 && (r.precipitationMm>0  || r.precipitationMm==0)) {
            return true;
        }
        return false;
    }

    record CityWeatherSummary(
            String city,
            long dayCount,
            double avgTemp,
            double avgPrecipitation,
            double maxTemp
    ) {}

    private static List<String> readCsvRows(String fileName) throws IOException {
        InputStream in = WeatherDataScienceExercise.class.getResourceAsStream(fileName);
        if (in == null) {
            throw new NoSuchFileException("Classpath resource not found: " + fileName);
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            return reader.lines().toList();
        }
    }
}
