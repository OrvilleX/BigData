package bigdata.vip56.cn;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {
    private static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private int airTemperature;
    private String quality;

    public void parse(String record) {
        year = record.substring(0, 4);
        String airTemperatureString;

        airTemperatureString = record.substring(4, 8);
        airTemperature = Integer.parseInt(airTemperatureString);
        quality = record.substring(8, 10);
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidTemperature() {
        return airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
    }

    public String getYear() {
        return year;
    }

    public int getAirTemperature() {
        return airTemperature;
    }
}