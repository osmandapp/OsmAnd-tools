package net.osmand.swing;

import java.io.File;

import net.osmand.NativeJavaRendering;

public class NativeSwingRendering {


	public static NativeJavaRendering getDefaultFromSettings() {
		return NativeJavaRendering.getDefault(
				DataExtractionSettings.getSettings().getNativeLibFile(), 
				DataExtractionSettings.getSettings().getBinaryFilesDir(), 
				findFontFolder());
	}
	
	public static String findFontFolder() {
		// "fonts" for *.zip, "OsmAndMapCreator/fonts" for IDE
		String[] folders = {"fonts", "OsmAndMapCreator/fonts"};
		for (String d : folders) {
			File directory = new File(d);
			if (directory.isDirectory()) {
				File[] files = directory.listFiles();
				for (File f : files) {
					if (f.getName().contains(".ttf") || f.getName().contains(".otf")) {
						return d;
					}
				}
			}
		}
		return "fonts";
	}
}
