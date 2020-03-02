package com.obzen.spark.parser;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

public class LogDetailParserTest {
    private static final Logger logger = LoggerFactory.getLogger(LogDetailParserTest.class);
    private SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    @Test
    public void parsingDate() {
        String dt = "20180801064000.225";
        //String dt = "20180822164000";
        List<String> weekName = Arrays.asList("일", "월", "화", "수", "목", "금", "토");

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss.SSS");
        //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        DateTimeFormatter hour = DateTimeFormatter.ofPattern("HH");
        DateTimeFormatter month = DateTimeFormatter.ofPattern("MM");
        DateTimeFormatter week = DateTimeFormatter.ofPattern("e");
        LocalDateTime dateTime = LocalDateTime.parse(dt, formatter);
        ZonedDateTime timezone = dateTime.atZone(ZoneId.of("Asia/Seoul"));

        System.out.println("date time: " + dateTime);
        System.out.println("month: " + dateTime.format(month));
        System.out.println("week: " + weekName.get(Integer.parseInt(dateTime.format(week)) - 1));
        System.out.println("hour: " + dateTime.format(hour));

        System.out.println("zoned time: " + timezone);

    }

    @Test
    public void simpleDateFormat() throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        String dt = "20180801064000225";
        Date dateTime = formatter.parse(dt);
        long timestamp = dateTime.getTime();

        SimpleDateFormat month = new SimpleDateFormat("MM");
        SimpleDateFormat week = new SimpleDateFormat("EEE");
        SimpleDateFormat hour = new SimpleDateFormat("HH");

        System.out.println("date: " + dateTime);
        System.out.println("month: " + month.format(new Date(timestamp)));
        System.out.println("week: " + week.format(new Date(timestamp)));
        System.out.println("hour: " + hour.format(new Date(timestamp)));
    }

    @Test
    public void testDateParsing() {
        SimpleDateFormat hourF = new SimpleDateFormat("HH");
        SimpleDateFormat weekF = new SimpleDateFormat("EEE");

        String t1 = "20180822154600016";
        String t2 = "20180822184700047";

        System.out.println(hourF.format(new Date(parseTimeMillis(t1))));
        System.out.println(hourF.format(new Date(parseTimeMillis(t2))));
        System.out.println(format.format(new Date(parseTimeMillis(t1))));
    }

    private long parseTimeMillis(String eventTimestamp) {
        long millis = 0L;
        try {
            millis = format.parse(eventTimestamp).getTime();
        } catch (ParseException e) {
            logger.error(String.format("Wrong date format! input:%s", eventTimestamp), e);
        }
        return millis;
    }

    @Test
    public void testParsingCSV() throws URISyntaxException, IOException {
        //Pattern pattern = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        Pattern pattern = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        Files.lines(Paths.get(ClassLoader
                .getSystemResource("data/ProductDisplayInfo.csv")
                .toURI()))
                .forEach(line -> {
                            String[] data = pattern.split(line, -1);
                            System.out.println("org:" + Arrays.deepToString(data));

                            for (int i = 0; i < data.length; i++)
                                data[i] = removeQuotes(data[i]);
                            System.out.println("cnv:" + Arrays.deepToString(data));
                        }
                )

        ;


    }

    private String removeQuotes(String data) {
        if (data.startsWith("\""))
            data = data.substring(1);
        if (data.endsWith("\""))
            data = data.substring(0, data.lastIndexOf("\""));

        return data;
    }

    @Test
    public void testMap() {
        Map<String, Value> map = new HashMap<>();
        map.put("A", new Value(1));
        map.put("B", new Value(1));
        map.put("C", new Value(1));

        Value o = map.get("A");
        o.addValue(2);
        //map.put("A", o);

        map.forEach((k, v) ->
                System.out.println(String.format(
                        "k:%s, v:%s"
                        , k, v
                )));
    }

    class Value {
        public int v;

        public Value(int i) {
            this.v = i;
        }

        public void addValue(int i) {
            this.v = this.v + i;
        }

        @Override
        public String toString() {
            return "v=" + v;
        }
    }

    @Test
    public void printTimeZone() {
        ZoneId.getAvailableZoneIds().forEach(System.out::println);
    }

}