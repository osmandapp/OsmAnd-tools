package net.osmand.obf.diff;


import net.osmand.IProgress;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.util.Algorithms;
import org.xmlpull.v1.XmlPullParserException;

import javax.xml.stream.XMLStreamException;
import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class RelationDiffGenerator {

    public static void main(String[] args) {

        if(args.length == 1 && args[0].equals("test")) {
            args = new String[5];
            List<String> s = new ArrayList<String>();
            s.add("/Users/ivan/OsmAnd/tmp/live1111/before_rel.osm");
            s.add("/Users/ivan/OsmAnd/tmp/live1111/after_rel.osm");
            s.add("/Users/ivan/OsmAnd/tmp/live1111/diff.osm");
            s.add("/Users/ivan/OsmAnd/tmp/live1111/after.osm.gz");
            s.add("/Users/ivan/OsmAnd/tmp/live1111/_after_rel_m.osm");
            args = s.toArray(new String[0]);
        } else if (args.length < 4) {
            System.out.println("Usage: <path before_rel.osm.gz file> " +
                "<path to after_rel.osm.gz file> " +
                "<path to _diff.osm file> " +
                "<path to after.osm.gz file> " +
                "<path to write result> ");
            System.exit(1);
        }
        String start = args[0];
        String end = args[1];
        String diff = args[2];
        String result = "";
        String otherAfter = "";
        if (args.length == 5) {
            otherAfter = args[3];
            result = args[4];
        } else {
            result = args[3];
        }

        try {
            RelationDiffGenerator rdg = new RelationDiffGenerator();
            File startFile = new File(start);
            File endFile = new File(end);
            File targetFile = new File(result);
            File diffFile = new File(diff);
            File otherAfterFile = null;
            if (!Algorithms.isEmpty(otherAfter)) {
                otherAfterFile = new File(otherAfter);
            }
            rdg.compareOsmFiles(startFile, endFile, diffFile, otherAfterFile, targetFile);
        } catch (IOException | XmlPullParserException | XMLStreamException e) {
            e.printStackTrace();
        }
    }

    private void compareOsmFiles(File start, File end, File diffFile, File otherAfterFile, File targetFile) throws IOException, XmlPullParserException, XMLStreamException {

        long time = System.currentTimeMillis();

        InputStream startIs;
        InputStream endIs;
        if(start.getName().endsWith(".gz")) {
            startIs = new GZIPInputStream(new FileInputStream(start));
        } else {
            startIs = new FileInputStream(start);
        }
        if(end.getName().endsWith(".gz")) {
            endIs = new GZIPInputStream(new FileInputStream(end));
        } else {
            endIs = new FileInputStream(end);
        }

        InputStream startStream = new BufferedInputStream(startIs, 8192 * 4);
        InputStream endStream = new BufferedInputStream(endIs, 8192 * 4);
        OsmBaseStorage startStorage = new OsmBaseStorage();
        OsmBaseStorage endStorage = new OsmBaseStorage();
        startStorage.parseOSM(startStream, IProgress.EMPTY_PROGRESS);
        long t1 = System.currentTimeMillis() - time;
        System.out.println("Parse " + start.getName() + ": " + (t1 / 1000) + " sec.");
        endStorage.parseOSM(endStream, IProgress.EMPTY_PROGRESS);
        long t2 = System.currentTimeMillis() - time - t1;
        System.out.println("Parse " + end.getName() + ": " + (t2 / 1000) + " sec.");
        startStream.close();
        endStream.close();
        Map<EntityId, Entity> startEntities = startStorage.getRegisteredEntities();
        Map<EntityId, Entity> endEntities = endStorage.getRegisteredEntities();
        int statisticAddedMembers = 0;

        Map<EntityId, Entity> otherAfterEntities = new HashMap<>();
        if (otherAfterFile != null) {
            time = System.currentTimeMillis();
            InputStream otherAfterIs;
            if(otherAfterFile.getName().endsWith(".gz")) {
                otherAfterIs = new GZIPInputStream(new FileInputStream(otherAfterFile));
            } else {
                otherAfterIs = new FileInputStream(otherAfterFile);
            }
            InputStream otherAfterStream = new BufferedInputStream(otherAfterIs, 8192 * 4);
            OsmBaseStorage otherAfterStorage = new OsmBaseStorage();
            otherAfterStorage.parseOSM(otherAfterStream, IProgress.EMPTY_PROGRESS);
            long t3 = System.currentTimeMillis() - time;
            System.out.println("Parse " + otherAfterFile.getName() + ": " + (t3 / 1000) + " sec.");
            otherAfterStream.close();
            otherAfterEntities = otherAfterStorage.getRegisteredEntities();
        }

        Set<EntityId> deletedObjIds = new HashSet<>();
        if (diffFile != null) {
            try {
                DiffParser.fetchModifiedIds(diffFile, null, deletedObjIds, null);
            } catch (IOException | XmlPullParserException e) {
                e.printStackTrace();
            }
        }
        long t3 = System.currentTimeMillis() - time - t2;
        System.out.println("Parse _diff.osm in " + (t3 / 1000) + " sec.");

        for (Map.Entry<EntityId, Entity> e : startEntities.entrySet()) {
            Entity.EntityType entityType = e.getKey().getType();
            if (entityType != Entity.EntityType.NODE && entityType != Entity.EntityType.WAY) {
                continue;
            }
            if (endEntities.containsKey(e.getKey())) {
                continue;
            }
            if (otherAfterEntities.containsKey(e.getKey())) {
                endStorage.registerEntity(otherAfterEntities.get(e.getKey()), null);
                statisticAddedMembers++;
            } else if (!deletedObjIds.contains(e.getKey())) {
                endStorage.registerEntity(e.getValue(), null);
                statisticAddedMembers++;
            }
        }
        long t4 = System.currentTimeMillis() - time - t3;
        System.out.println("Added " + statisticAddedMembers + " node/ways: in " + (t4 / 1000) + " sec.");

        File targetFileSource = targetFile.getName().endsWith(".gz") ? new File(targetFile.getAbsolutePath().replace(".gz", "")) : targetFile;
        targetFileSource.delete();
        OutputStream output = new FileOutputStream(targetFileSource);
        OsmStorageWriter w = new OsmStorageWriter();
        w.saveStorage(output, endStorage, null, true);
        output.close();

        if (targetFile.getName().endsWith(".gz")) {
            FileInputStream fis = new FileInputStream(targetFileSource);
            GZIPOutputStream gzout = new GZIPOutputStream(new FileOutputStream(targetFile));
            Algorithms.streamCopy(fis, gzout);
            fis.close();
            gzout.close();
            targetFileSource.delete();
        }
        long t5 = System.currentTimeMillis() - time - t4;
        System.out.println("Wrote result into " + targetFile.getName() + " in " + (t5 / 1000)  + " sec.");
    }
}
