package net.osmand.router;

import net.osmand.swing.NativeSwingRendering;

public class TestRoutingWithNative {
	
	public static void main(String[] args) throws Exception {
		TestRouting.lib = NativeSwingRendering.getDefaultFromSettings();
		TestRouting.main(args);
	}

}
