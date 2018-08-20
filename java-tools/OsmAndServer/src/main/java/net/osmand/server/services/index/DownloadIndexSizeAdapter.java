package net.osmand.server.services.index;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.util.Locale;

public class DownloadIndexSizeAdapter extends XmlAdapter<String, Double> {

	private static final double MB =  1 << 20;

	@Override
	public Double unmarshal(String v) throws Exception {
		return Double.parseDouble(v);
	}

	@Override
	public String marshal(Double v) throws Exception {
		return String.format(Locale.US, "%.1f", v / MB);
	}
}
