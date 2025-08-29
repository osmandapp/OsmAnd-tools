package net.osmand.reviews.mangrove;

import com.google.common.collect.ImmutableList;
import net.osmand.PlatformUtil;
import net.osmand.binary.MapZooms;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.map.WorldRegion;
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.reviews.ReviewedPlace;
import net.osmand.reviews.Review;
import net.osmand.reviews.ReviewJsonCodec;
import net.osmand.reviews.Tags;
import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlSerializer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.Month;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Tools for working with reviews from <a href="https://mangrove.reviews">mangrove.reviews</a>.
 */
public final class MangroveReviews {
    private static final Log log = PlatformUtil.getLog(MangroveReviews.class);

    public static void main(String[] args) throws Exception {
        File outputDir = null;
        for (String arg : args) {
            String val = arg.substring(arg.indexOf("=") + 1);
            if (arg.startsWith("--dir=")) {
                outputDir = new File(val);
            } else {
                throw new IllegalArgumentException(String.format("unexpected argument: '%s'. args: %s", arg, Arrays.toString(args)));
            }
        }
        if (outputDir == null) {
            throw new IllegalArgumentException("--dir=<output-dir> argument must be specified");
        }
        generateTestFile(outputDir);
    }

    private static void generateTestFile(File outputDir) throws IOException, URISyntaxException, SQLException, XmlPullParserException, InterruptedException {
        List<ReviewedPlace> testReviews = ImmutableList.of(
                new ReviewedPlace(52.2219219, 21.0142540,9028264713L,
                        ImmutableList.of(
                                new Review(
                                        "yhjkE6T9Xkq-ZXIGsBPJtexR9ISOSljxqXRcGcBBPw0bUmmTl-OFSDI1ZP4btkNk8knMWajZpAHwzdPQHFyScw",
                                        "Vintage cocktails made with ingredients that are no longer available and have been recreated from scratch. Amazing barrel-aged cocktails.",
                                        100,
                                        "enigal",
                                        LocalDate.of(2025, Month.JULY, 8),
                                        new URI("https://mangrove.reviews/list?signature=yhjkE6T9Xkq-ZXIGsBPJtexR9ISOSljxqXRcGcBBPw0bUmmTl-OFSDI1ZP4btkNk8knMWajZpAHwzdPQHFyScw")),
                                new Review(
                                        "test1",
                                        "I could not find the place at all",
                                        40,
                                        "some rando",
                                        LocalDate.of(2025, Month.JULY, 22),
                                        new URI("https://blog.mmakowski.com"))
                        )));
        File osmGz = new File(outputDir, WorldRegion.WORLD + ".reviews.osm.gz");
        File obf = new File(outputDir, WorldRegion.WORLD + ".reviews.obf");
        generateOsmFile(testReviews, osmGz);
        generateObf(osmGz, obf);
    }

    private static void generateOsmFile(List<ReviewedPlace> places, File outputFile) throws IOException {
        // TODO: factor out the common bits from Wikipedia and Reviews
        FileOutputStream out = new FileOutputStream(outputFile);
        GZIPOutputStream gzStream = new GZIPOutputStream(out);
        XmlSerializer serializer = new org.kxml2.io.KXmlSerializer();
        serializer.setOutput(gzStream, "UTF-8");
        serializer.startDocument("UTF-8", true);
        serializer.startTag(null, "osm");
        serializer.attribute(null, "version", "0.6");
        serializer.attribute(null, "generator", "OsmAnd");
        serializer.setFeature("http://xmlpull.org/v1/doc/features.html#indent-output", true);
        ReviewJsonCodec reviewCodec = new ReviewJsonCodec();

        for (ReviewedPlace place : places) {
            serializer.startTag(null, "node");
            serializer.attribute(null, "visible", "true");
            serializer.attribute(null, "id", String.valueOf(place.osmId()));
            serializer.attribute(null, "lat", String.valueOf(place.lat()));
            serializer.attribute(null, "lon", String.valueOf(place.lon()));
            addTag(serializer, Tags.REVIEWS_MARKER_TAG, Tags.REVIEWS_MARKER_VALUE);
            addTag(serializer, Tags.REVIEWS_KEY, reviewCodec.toJson(place.reviews()));
            // TODO: summary info
            serializer.endTag(null, "node");
        }
        serializer.endDocument();
        serializer.flush();
        gzStream.close();
    }

    private static void addTag(XmlSerializer serializer, String key, String value) throws IOException {
        serializer.startTag(null, "tag");
        serializer.attribute(null, "k", key);
        serializer.attribute(null, "v", value);
        serializer.endTag(null, "tag");
    }

    private static void generateObf(File osmGz, File obf) throws IOException, SQLException, InterruptedException, XmlPullParserException {
        IndexCreatorSettings settings = new IndexCreatorSettings();
        settings.indexMap = false;
        settings.indexAddress = false;
        settings.indexPOI = true;
        settings.indexTransport = false;
        settings.indexRouting = false;

        IndexCreator creator = new IndexCreator(obf.getParentFile(), settings);
        new File(obf.getParentFile(), IndexCreator.TEMP_NODES_DB).delete();
        creator.setMapFileName(obf.getName());
        creator.generateIndexes(osmGz,
                new ConsoleProgressImplementation(1), null, MapZooms.getDefault(),
                new MapRenderingTypesEncoder(obf.getName()), log);
    }

    private MangroveReviews() {
    }
}
