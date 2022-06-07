package net.osmand.osm;

import org.apache.commons.lang3.ArrayUtils;
import java.util.ArrayList;
import java.util.Map;

public class OsmcSymbol {
	private String waycolor = "";
	private String background = "";
	private String foreground = "";
	private String foreground2 = "";
	private String text = "";
	private String textcolor = "";

	/**
	 * https://wiki.openstreetmap.org/wiki/Key:osmc:symbol#Examples
	 * osmc:symbol=waycolor:background[:foreground][[:foreground2]:text:textcolor]
	 * @param value - value of tag "osmc:symbol"
	 */
	public OsmcSymbol(String value) {
		String[] tokens = value.split(":", 6);
		tokens = toLowerCaseOsmcTags(tokens);
		int tokensLength = tokens.length;

		if (tokensLength > 0) {
			waycolor = isColor(tokens[0]) ? tokens[0] : waycolor;
			if (tokensLength > 1) {
				if (isBackground(tokens[1])) {
					//get background
					background = tokens[1];
				} else if (isForeground(tokens[1])) {
					//get foreground when hasn't background
					foreground = tokens[1];
				}
			}
			if (tokensLength > 2) {
				int textInd = getTextIndex(tokens);
				getText(tokens, textInd);
				getForegrounds(tokens, textInd);
			}
			addTextLengthToBgAndFrg();
		}
	}

	public OsmcSymbol(String background, String text, String textcolor) {
		this.background = background;
		this.text = text;
		this.textcolor = textcolor;
		addTextLengthToBgAndFrg();
	}

	public void addOsmcNewTags(Map<String, String> tags) {
		if (!waycolor.isEmpty()) {
			tags.put("osmc_waycolor", waycolor);
		}
		if (!background.isEmpty()) {
			tags.put("osmc_background", background);
			tags.put("osmc_stub_name", ".");
		} else {
			if (!waycolor.isEmpty()) {
				//osmc:symbol=blue:shell_modern equals blue:blue:shell_modern
				tags.put("osmc_background", waycolor);
			} else {
				tags.put("osmc_background", "white");
			}
			tags.put("osmc_stub_name", ".");
		}
		if (!foreground.isEmpty()) {
			tags.put("osmc_foreground", foreground);
		}
		if (!text.isEmpty()) {
		    text = text.substring(0, Math.min(text.length(), 7));
			tags.put("osmc_text", text);
			if (!textcolor.isEmpty()) {
				tags.put("osmc_textcolor", textcolor);
			}
		}
		if (!foreground2.isEmpty()) {
			tags.put("osmc_foreground2", foreground2);
		}
	}

	private void addTextLengthToBgAndFrg() {
		if (text.isEmpty()) {
			return;
		}
		int textLength = text.codePointCount(0, text.length());
		textLength = Math.min(textLength, 4);
		if (textLength > 1) {
			if (!background.isEmpty()) {
				background += "_" + textLength;
			}
			if (!foreground.isEmpty()) {
				foreground += "_" + textLength;
			}
		}
	}

	private boolean hasForeground() {
		return !foreground.isEmpty();
	}

	private String[] toLowerCaseOsmcTags(String[] tokens) {
		ArrayList<String> res = new ArrayList<>();
		for (String token : tokens) {
			if (!isText(token)) {
				res.add(token.toLowerCase());
			} else {
				res.add(token);
			}
		}
		return res.toArray(new String[0]);
	}

	private boolean isColor(String s) {
		return s.equals("black")
				|| s.equals("blue")
				|| s.equals("green")
				|| s.equals("red")
				|| s.equals("white")
				|| s.equals("yellow")
				|| s.equals("orange")
				|| s.equals("purple")
				|| s.equals("brown")
				|| s.equals("gray");
	}

	private boolean isForeground(String s) {
		return s.equals("ammonit")
				|| s.equals("bridleway")
				|| s.equals("heart")
				|| s.equals("hiker")
				|| s.equals("mine")
				|| s.equals("planet")
				|| s.equals("shell")
				|| s.equals("shell_modern")
				|| s.equals("tower")
				|| s.equals("wolfshook") || hasColorPrefix(s);
	}

	private boolean isBackground(String s) {
		return hasColorPrefix(s) || isColor(s);
	}

	private boolean hasColorPrefix(String s) {
		if (s.contains("_")) {
			String[] arr = s.split("_");
			if (arr.length == 0) {
				// string like "_" "__"
				return false;
			}
			return isColor(arr[0]);
		}
		return false;
	}

	private void getText(String[] tokens, int textInd) {
		if (textInd != -1 && textInd + 1 < tokens.length && isColor(tokens[textInd + 1])) {
			text = tokens[textInd];
			textcolor = tokens[textInd + 1];
		}
	}

	private boolean isText(String s) {
		if (!s.isEmpty()) {
			char first = s.charAt(0);
			return Character.isDigit(first) || Character.isUpperCase(first);
		}
		return false;
	}

	private int getTextIndex(String[] tokens) {
		for (String token : tokens) {
			if (isText(token) || isSpecSymbol(token.trim())) {
				return ArrayUtils.indexOf(tokens, token);
			}
		}
		return -1;
	}

	private boolean isSpecSymbol(String s) {
		if (!s.isEmpty()) {
			char first = s.charAt(0);
			return !Character.isLetterOrDigit(first);
		}
		return false;
	}

	private void getForegrounds(String[] tokens, int textInd) {
		int foregroundIndex = -1;
		int foregraundIndex2 = -1;

		if (textInd != -1) {
			if (hasForeground()) {
				foregraundIndex2 = (textInd - 1 >= 2 && isForeground(tokens[textInd - 1])) ? textInd - 1 : -1;
			} else {
				if (textInd - 1 == 3 && isForeground(tokens[3])) {
					foregroundIndex = isForeground(tokens[2]) ? 2 : -1;
					foregraundIndex2 = 3;
				} else if (textInd - 1 == 2 && isForeground(tokens[2])) {
					foregroundIndex = 2;
				}
			}
		} else {
			if (hasForeground()) {
				foregroundIndex = ArrayUtils.indexOf(tokens, foreground);
				if (isForeground(tokens[foregroundIndex + 1])) {
					foregraundIndex2 = foregroundIndex + 1;
				}
			} else {
				foregroundIndex = isForeground(tokens[2]) ? 2 : -1;
				if (tokens.length > 3) {
					foregraundIndex2 = isForeground(tokens[3]) ? 3 : -1;
				}
			}
		}
		foreground = foregroundIndex != -1 ? tokens[foregroundIndex] : foreground;
		foreground2 = foregraundIndex2 != -1 ? tokens[foregraundIndex2] : foreground2;
	}
}
