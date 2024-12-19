package net.osmand.obf.diff;

import net.osmand.PlatformUtil;
import net.osmand.osm.edit.Entity;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.zip.GZIPInputStream;

public class DiffParser {

    private static final String ATTR_ID = "id";
    private static final String TYPE_RELATION = "relation";
    private static final String TYPE_WAY = "way";
    private static final String TYPE_NODE = "node";
    private static final String TYPE_ACTION = "action";
    private static final String ATTR_ACTION_TYPE = "type";

    private enum ActionType {
        CREATE("create"),
        DELETE("delete"),
        MODIFY("modify");
        final String type;

        ActionType(String type) {
            this.type = type;
        }

        static ActionType getType(String typeStr) {
            for (ActionType at : values()) {
                if (at.type.equals(typeStr)) {
                    return at;
                }
            }
            return null;
        }
    }

    public static void fetchModifiedIds(File diff, Set<Entity.EntityId> all, Set<Entity.EntityId> deleted, Set<Entity.EntityId> modified) throws IOException, XmlPullParserException {
        InputStream fis;
        if (diff.getName().endsWith(".gz")) {
            fis = new GZIPInputStream(new FileInputStream(diff));
        } else {
            fis = new FileInputStream(diff);
        }
        XmlPullParser parser = PlatformUtil.newXMLPullParser();
        parser.setInput(fis, "UTF-8");
        int tok;
        ActionType actionType = null;
        while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
            if (tok == XmlPullParser.START_TAG) {
                String name = parser.getName();
                if (TYPE_ACTION.equals(name)) {
                    actionType = ActionType.getType(parser.getAttributeValue("", ATTR_ACTION_TYPE));
                }
                Entity.EntityId entityId = null;
                if (TYPE_NODE.equals(name)) {
                    entityId = new Entity.EntityId(Entity.EntityType.NODE, parseLong(parser, ATTR_ID, -1));
                } else if (TYPE_WAY.equals(name)) {
                    entityId = new Entity.EntityId(Entity.EntityType.WAY, parseLong(parser, ATTR_ID, -1));
                } else if (TYPE_RELATION.equals(name)) {
                    entityId = new Entity.EntityId(Entity.EntityType.RELATION, parseLong(parser, ATTR_ID, -1));
                }
                if (entityId != null) {
                    if (all != null) {
                        all.add(entityId);
                    }
                    if (deleted != null && actionType != null && actionType != ActionType.MODIFY) {
                        deleted.add(entityId);
                    }
                    if (modified != null && actionType != null && actionType == ActionType.MODIFY) {
                        modified.add(entityId);
                    }
                }
            } else if (tok == XmlPullParser.END_TAG) {
                if (TYPE_ACTION.equals(parser.getName()) && actionType != null) {
                    actionType = null;
                }
            }
        }
    }

    protected static long parseLong(XmlPullParser parser, String name, long defVal) {
        long ret = defVal;
        String value = parser.getAttributeValue("", name);
        if (value == null) {
            return defVal;
        }
        try {
            ret = Long.parseLong(value);
        } catch (NumberFormatException ignored) {
        }
        return ret;
    }
}
