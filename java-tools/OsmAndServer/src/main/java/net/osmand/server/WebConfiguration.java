package net.osmand.server;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

@Configuration
public class WebConfiguration {

    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm";

    @Bean
    public SimpleDateFormat dateFormat() {
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_PATTERN);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat;
    }
}
