package net.osmand.router;

import org.junit.Ignore;

import net.osmand.swing.NativeSwingRendering;

@Ignore
public class TestRoutingWithNative {

	public static void main(String[] args) throws Exception {
		TestRouting.lib = NativeSwingRendering.getDefaultFromSettings();
		TestRouting.main(args);
	}

}
