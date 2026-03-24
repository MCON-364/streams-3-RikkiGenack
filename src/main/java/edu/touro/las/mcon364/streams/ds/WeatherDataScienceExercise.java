package edu.touro.las.mcon364.streams.ds;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;


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
        List<String> rows = Files.readAllLines(Path.of("noaa_weather_sample_200_rows.csv"));

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

        // TODO 6:
        // Partition rows into freezing days (temperature <= 0)
        // and non-freezing days (temperature > 0).

        // TODO 7:
        // Create a Set<String> of all distinct cities.

        // TODO 8:
        // Find the wettest single day.

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

        throw new UnsupportedOperationException("TODO: implement parseRow");
    }

    static boolean isValid(WeatherRecord r) {
        // TODO:
        // Keep only rows where:
        // - temperature is between -60 and 60
        // - humidity is between 0 and 100
        // - precipitation is >= 0

        throw new UnsupportedOperationException("TODO: implement isValid");
    }

    record CityWeatherSummary(
            String city,
            long dayCount,
            double avgTemp,
            double avgPrecipitation,
            double maxTemp
    ) {}
}
