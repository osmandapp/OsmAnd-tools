package net.osmand.osm.util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import info.bliki.htmlcleaner.ContentToken;
import info.bliki.htmlcleaner.TagNode;
import info.bliki.htmlcleaner.TagToken;
import info.bliki.htmlcleaner.Utils;
import info.bliki.wiki.filter.Encoder;
import info.bliki.wiki.filter.WikipediaParser;
import info.bliki.wiki.model.Configuration;
import info.bliki.wiki.model.ImageFormat;
import info.bliki.wiki.model.WikiModel;
import info.bliki.wiki.tags.PTag;

public class CustomWikiModel extends WikiModel {
		
	private PreparedStatement prep;

	public CustomWikiModel(String imageBaseURL, String linkBaseURL, String folder, PreparedStatement prep) {
		super(imageBaseURL, linkBaseURL);
		this.prep = prep;
	}
	
	@Override
	protected String createImageName(ImageFormat imageFormat) {
		String imageName = imageFormat.getFilename();
				
		if (imageName.endsWith(".svg")) {
			imageName += ".png";
		}
		imageName = Encoder.encodeUrl(imageName);
		if (replaceColon()) {
			imageName = imageName.replace(':', '/');
		}
		return imageName;
	}
	
	@Override
	public void parseInternalImageLink(String imageNamespace,
			String rawImageLink) {
		String imageName = rawImageLink.split("\\|")[0].replaceFirst("File:", "");
		if (imageName.isEmpty()) {
			return;
		}
		String imageHref = getWikiBaseURL();
		ImageFormat imageFormat = ImageFormat.getImageFormat(rawImageLink, imageNamespace);
		String link = imageFormat.getLink();
		if (link != null) {
			if (link.length() == 0) {
				imageHref = "";
			} else {
				String encodedTitle = encodeTitleToUrl(link, true);
				imageHref = imageHref.replace("${title}", encodedTitle);
			}

		} else {
			if (replaceColon()) {
				imageHref = imageHref.replace("${title}", imageNamespace + '/' + imageName);
			} else {
				imageHref = imageHref.replace("${title}", imageNamespace + ':' + imageName);
			}
		}
		String imageSrc = getImageLinkFromDB(imageName);
		if (imageSrc.isEmpty()) {
			return;
		}	
		String type = imageFormat.getType();
		TagToken tag = null;
		if ("thumb".equals(type) || "frame".equals(type)) {
			if (fTagStack.size() > 0) {
				tag = peekNode();
			}
			reduceTokenStack(Configuration.HTML_DIV_OPEN);
		}
		appendInternalImageLink(imageHref, imageSrc, imageFormat);
		if (tag instanceof PTag) {
			pushNode(new PTag());
		}
	}

	public String getImageLinkFromDB(String imageName) {
		String imageSrc = "";
		try {
			prep.setString(1, imageName);
			ResultSet rs = prep.executeQuery();
			while (rs.next()) {
				imageSrc = rs.getString("image_url");
			}
			prep.clearParameters();
		} catch (SQLException e) {}
		return imageSrc;
	}
	
	@Override
	public boolean isValidUriScheme(String uriScheme) {
		return (uriScheme.contains("http") || uriScheme.contains("https") 
				|| uriScheme.contains("ftp") || uriScheme.contains("mailto") 
				|| uriScheme.contains("tel") || uriScheme.contains("geo"));
	}
	
	@Override
    public void appendExternalLink(String uriSchemeName, String link,
            String linkName, boolean withoutSquareBrackets) {
        link = Utils.escapeXml(link, true, false, false);
        if (!uriSchemeName.equals("http") && !uriSchemeName.equals("https") 
        		&& !uriSchemeName.equals("ftp")) {
        	if (uriSchemeName.equals("tel")) {
        		link = link.replaceAll("/", " ").replaceAll("o", "(").replaceAll("c", ")");
        		linkName = link.replaceFirst(uriSchemeName + ":", "");
        	} else {
        		linkName = uriSchemeName.equals("geo") ? "Open on map" : 
            		link.replaceFirst(uriSchemeName + ":", "");
        	}
        }
        TagNode aTagNode = new TagNode("a");
        aTagNode.addAttribute("href", link, true);
        aTagNode.addAttribute("rel", "nofollow", true);
        if (withoutSquareBrackets) {
            aTagNode.addAttribute("class", "external free", true);
            append(aTagNode);
            aTagNode.addChild(new ContentToken(linkName));
        } else {
            String trimmedText = linkName.trim();
            if (trimmedText.length() > 0) {
                pushNode(aTagNode);
                if (linkName.equals(link)
                // protocol-relative URLs also get auto-numbered if there is no
                // real
                // alias
                        || (link.length() >= 2 && link.charAt(0) == '/'
                                && link.charAt(1) == '/' && link.substring(2)
                                .equals(linkName))) {
                    aTagNode.addAttribute("class", "external autonumber", true);
                    aTagNode.addChild(new ContentToken("["
                            + (++fExternalLinksCounter) + "]"));
                } else {
                    aTagNode.addAttribute("class", "external text", true);
                    WikipediaParser.parseRecursive(trimmedText, this, false,
                            true);
                }
                popNode();
            }
        }
    }
}
