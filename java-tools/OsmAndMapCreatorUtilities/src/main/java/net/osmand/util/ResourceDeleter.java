package net.osmand.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class ResourceDeleter {

	private static Document parseResources(File f, Set<String> keys) throws SAXException, IOException, ParserConfigurationException {
		Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new InputSource(new FileInputStream(f)));
		if (keys != null) {
			NodeList list = doc.getElementsByTagName("string");
			for (int j = 0; j < list.getLength(); j++) {
				Element item = (Element) list.item(j);
				keys.add(item.getAttribute("name"));
			}
		}
		return doc;
	}

	public static void main(String[] args) throws SAXException, IOException, ParserConfigurationException, TransformerException {
		if (args.length == 0) {
			args = new String[] { "../../../android/OsmAnd/res" };
		}
		File f = new File(args[0]);
		Set<String> mainkeys = new LinkedHashSet<String>();
		parseResources(new File(f, "values/strings.xml"), mainkeys);
		File[] lf = f.listFiles();
		for(int i = 0; i < lf.length; i++) {
			File file = new File(lf[i], "strings.xml");
			if(lf[i].getName().startsWith("values-") && file.exists()) {
				Set<String> keys = new LinkedHashSet<String>();


				Document doc = parseResources(file, keys);
				keys.removeAll(mainkeys);
				System.out.println(lf[i].getName() + " - " + keys);

				NodeList list = doc.getElementsByTagName("string");
				for (int j = 0; j < list.getLength(); j++) {
					Element item = (Element) list.item(j);
					if(keys.contains(item.getAttribute("name"))) {
						item.getParentNode().removeChild(item);
					}
				}

				TransformerFactory transformerFactory = TransformerFactory.newInstance();
				Transformer transformer = transformerFactory.newTransformer();
				DOMSource source = new DOMSource(doc);
				StreamResult result = new StreamResult(file);

				// Output to console for testing
				// StreamResult result = new StreamResult(System.out);
				transformer.transform(source, result);
			}
		}
	}
}
