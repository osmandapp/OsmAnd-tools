package net.osmand.data.diff;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.PlatformUtil;
import net.osmand.osm.edit.Entity;

public class AugmentedDiffsInspector {

	public static void main(String[] args) throws XmlPullParserException, IOException {
		String file = args[0];
		File f = new File(file);
		if(f.isDirectory()) {
			for(File d : f.listFiles()) {
				if(d.getName().endsWith("gz")) {
					new AugmentedDiffsInspector().process(d);
				}
			}
		} else if(f.getName().endsWith("gz")){
			new AugmentedDiffsInspector().process(f);
		}
	}
	
	private static class Context {
		Map<String, Integer> changesets = new LinkedHashMap<String, Integer>();
	}

	private void process(File file) throws XmlPullParserException, IOException {
		XmlPullParser parser = PlatformUtil.newXMLPullParser();
		parser.setInput(new GZIPInputStream(new FileInputStream(file)), "UTF-8");
		int next;
		int modify = Entity.MODIFY_UNKNOWN;
		Context ctx = new Context();
		boolean old = false;
		while((next = parser.next()) != XmlPullParser.END_DOCUMENT){
			if(next == XmlPullParser.START_TAG) {
				String name = parser.getName();
				if("action".equals(name)) {
					String type = parser.getAttributeValue("", "type");
					if("modify".equals(type)) {
						modify = Entity.MODIFY_MODIFIED;
					} else if("delete".equals(type)) {
						modify = Entity.MODIFY_DELETED;
					} else if("create".equals(type)) {
						modify = Entity.MODIFY_CREATED;
					}
					old = false;
				} else if(name.equals("old")) {
					old = true;
				} else if(name.equals("new")) {
					old = false;
				} else if(name.equals("way") || name.equals("relation") || 
						name.equals("node")){
					processElement(parser, ctx, name, modify, old);
				}
			}
		}
		System.out.println(ctx);
	}

	private void processElement(XmlPullParser parser, Context ctx, String name, int modify, boolean old) {
		if(!old) {
			String changeset = parser.getAttributeValue("", "changeset");
			if(!ctx.changesets.containsKey(changeset)){
				ctx.changesets.put(changeset, 0);
			}
			ctx.changesets.put(changeset, ctx.changesets.get(changeset) + 1);
		}
//		while((next = parser.next()) != XmlPullParser.END_DOCUMENT){
	}
}
