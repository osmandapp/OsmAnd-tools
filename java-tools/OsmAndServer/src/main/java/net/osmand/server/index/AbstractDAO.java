package net.osmand.server.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipFile;

public abstract class AbstractDAO {
    private static final Log LOGGER = LogFactory.getLog(AbstractDAO.class);

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd.MM.yy");

    protected static final Path BASE_PATH = Paths.get("/var/www-download/");
    protected static final String FILES_PATTERN = "(?<d>\\D+)\\d?(?:\\.\\w+){1,3}";
    protected static final String FONTS_PATTERN = "(?<d>\\D+)(\\.\\w+\\.\\w+)";

    protected boolean validateZipFile(File file) {
        boolean validationResult = true;
        try {
            new ZipFile(file);
        } catch (IOException ex) {
            validationResult = false;
            LOGGER.error(ex.getMessage(), ex);
        }
        return validationResult;
    }

    protected String createDescriptionByPattern(String name, String pattern) {
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(name);
        if (m.find()) {
           String description = m.group("d");
           description = description.replaceAll("_", " ");
           return description.trim();
        }
        throw new IllegalArgumentException("Cannot create description from " + name);
    }

    protected double sizeInMBs(BasicFileAttributes bfa) {
        return bfa.size() / (1000d * 1000d);
    }

    protected long sizeInBytes(BasicFileAttributes bfa) {
        return bfa.size();
    }

    protected String formatDate(BasicFileAttributes bfa) {
        return DATE_FORMAT.format(new Date(bfa.creationTime().to(TimeUnit.MILLISECONDS)));
    }
}
