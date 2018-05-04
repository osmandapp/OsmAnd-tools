package net.osmand.osm.util;

import static info.bliki.wiki.tags.WPATag.CLASS;
import static info.bliki.wiki.tags.WPATag.HREF;
import static info.bliki.wiki.tags.WPATag.WIKILINK;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;

import com.google.gson.Gson;

import info.bliki.Messages;
import info.bliki.htmlcleaner.ContentToken;
import info.bliki.htmlcleaner.TagNode;
import info.bliki.htmlcleaner.TagToken;
import info.bliki.htmlcleaner.Utils;
import info.bliki.wiki.filter.Encoder;
import info.bliki.wiki.filter.SectionHeader;
import info.bliki.wiki.filter.WikipediaParser;
import info.bliki.wiki.model.Configuration;
import info.bliki.wiki.model.ITableOfContent;
import info.bliki.wiki.model.ImageFormat;
import info.bliki.wiki.model.WikiModel;
import info.bliki.wiki.tags.HTMLBlockTag;
import info.bliki.wiki.tags.HTMLTag;
import info.bliki.wiki.tags.PTag;
import info.bliki.wiki.tags.TableOfContentTag;
import info.bliki.wiki.tags.util.TagStack;


public class CustomWikiModel extends WikiModel {
	
	private Map<String, Map<String, Object>> dataMap;
	private String prevHead = "";
	private boolean preserveContents;
	

	public static final String ROOT_URL = "https://upload.wikimedia.org/wikipedia/commons/";
	private static final String PREFIX = "320px-";

	public CustomWikiModel(String imageBaseURL, String linkBaseURL, boolean preserveContents) {
		super(imageBaseURL, linkBaseURL);
		dataMap = new LinkedHashMap<>();
		this.preserveContents = preserveContents;
	}
	
	public String getContentsJson() {
		Map<String, Map<String, Map<String, Object>>> finalData = new LinkedHashMap<>();
		finalData.put("headers", dataMap);
		return new Gson().toJson(finalData);
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
		String imageName = rawImageLink.split("\\|")[0];
		imageName = imageName.substring(imageName.indexOf(":") + 1);
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
		String imageSrc = getThumbUrl(imageName);
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
		imageFormat.setType("thumbnail");
		imageFormat.setWidth(-1);
		appendInternalImageLink(imageHref, imageSrc, imageFormat);
		if (tag instanceof PTag) {
			pushNode(new PTag());
		}
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
        	boolean geo = uriSchemeName.equals("geo");
        	geo = geo ? aTagNode.addAttribute("class", "geo", true) : aTagNode.addAttribute("class", "external free", true);
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
                    aTagNode.addAttribute("class", link.contains(".wikipedia.org/wiki/") ? "geo" : "external text", true);
                    WikipediaParser.parseRecursive(trimmedText, this, false,
                            true);
                }
                popNode();
            }
        }
    }
	
	@Override
	public void appendInternalLink(String topic, String hashSection,
			String topicDescription, String cssClass, boolean parseRecursive) {
		appendInternalLink(topic, hashSection, topicDescription, cssClass,
				parseRecursive, true);
	}

	protected void appendInternalLink(String topic, String hashSection,
			String topicDescription, String cssClass, boolean parseRecursive,
			boolean topicExists) {
		String hrefLink;
		String description = topicDescription.trim();

		if (topic.length() > 0) {
			String title = Encoder.normaliseTitle(topic, true, ' ', true);
			if (hashSection == null) {
				String pageName = Encoder.normaliseTitle(fPageTitle, true, ' ',
						true);
				// self link?
				if (title.equals(pageName)) {
					HTMLTag selfLink = new HTMLTag("strong");
					selfLink.addAttribute("class", "selflink", false);
					pushNode(selfLink);
					selfLink.addChild(new ContentToken(description));
					popNode();
					return;
				}
			}

			String encodedtopic = encodeTitleToUrl(topic, true);
			if (replaceColon()) {
				encodedtopic = encodedtopic.replace(':', '/');
			}
			hrefLink = getWikiBaseURL().replace("${title}", encodedtopic);
			if (!topicExists) {
				if (cssClass == null) {
					cssClass = "new";
				}
				if (hrefLink.indexOf('?') != -1) {
					hrefLink += "&";
				} else {
					hrefLink += "?";
				}
				hrefLink += "action=edit&redlink=1";
				String redlinkString = Messages.getString(getResourceBundle(),
						Messages.WIKI_TAGS_RED_LINK,
						"${title} (page does not exist)");
				title = redlinkString.replace("${title}", title);
			}
			String toCompare = hrefLink.toLowerCase();
			if (!toCompare.contains(".jpg") && !toCompare.contains(".jpeg") 
					&& !toCompare.contains(".png") && !toCompare.contains(".gif") && !toCompare.contains(".svg")) {
				appendExternalLink("https", hrefLink, topicDescription, true);
			}
		} else if (hashSection != null) {
			TagNode aTagNode = new TagNode("a");
			String href = encodeTitleToUrl(topic, true);
	        if (hashSection != null) {
	            href = href + '#' + encodeTitleDotUrl(hashSection, false);
	        }
	        aTagNode.addAttribute(HREF, href, true);
	        if (cssClass != null) {
	            aTagNode.addAttribute(CLASS, cssClass, true);
	        }
	        pushNode(aTagNode);
	        aTagNode.addChild(new ContentToken(description));;
	        popNode();
		}
	}
	
	/**
     * Append a new head to the table of content
     */
    @SuppressWarnings("unchecked")
	@Override
    public ITableOfContent appendHead(String rawHead, int headLevel,
            boolean noToC, int headCounter, int startPosition, int endPosition) {
    	if (preserveContents) {
    		return super.appendHead(rawHead, headLevel, noToC, headCounter, startPosition, endPosition);
    	} else {
    		TagStack localStack = WikipediaParser.parseRecursive(rawHead.trim(),
                    this, true, true);
            HTMLBlockTag headTagNode = new HTMLBlockTag("h" + headLevel,
                    Configuration.SPECIAL_BLOCK_TAGS);
            TagNode spanTagNode = new TagNode("span");
            spanTagNode.addChildren(localStack.getNodeList());
            headTagNode.addChild(spanTagNode);
            String tocHead = headTagNode.getBodyString();
            String anchor = Encoder.encodeDotUrl(tocHead);
            createTableOfContent(false);
            if (fToCSet.contains(anchor)) {
                String newAnchor = anchor;
                for (int i = 2; i < Integer.MAX_VALUE; i++) {
                    newAnchor = anchor + '_' + Integer.toString(i);
                    if (!fToCSet.contains(newAnchor)) {
                        break;
                    }
                }
                anchor = newAnchor;
            }
            fToCSet.add(anchor);
            String trimmedHead = rawHead.trim();
            if (headLevel == 2) {
            	Map<String, Object> data = new LinkedHashMap<>();
            	data.put("link", "#" + anchor);
                dataMap.put(trimmedHead, data);
                prevHead = trimmedHead;
            } else if (headLevel == 3) {
    			Map<String, Object> data = dataMap.get(prevHead);
    			if (data != null) {
    				Map<String, Map<String, String>> subHeaders = (Map<String, Map<String, String>>) data.get("subheaders");
    				subHeaders = subHeaders == null ? new LinkedHashMap<String, Map<String, String>>() : subHeaders;
    				Map<String, String> link = new HashMap<>();
    				link.put("link", "#" + anchor);
    				subHeaders.put(trimmedHead, link);
    				data.put("subheaders", subHeaders);
    			}
    		}
            SectionHeader strPair = new SectionHeader(headLevel, startPosition,
                    endPosition, tocHead, anchor);
            if (getRecursionLevel() == 1) {
                buildEditLinkUrl(fSectionCounter++);
            }
            spanTagNode.addAttribute("class", "mw-headline", true);
            spanTagNode.addAttribute("id", anchor, true);
            
            append(headTagNode);
            return new TableOfContentTag("a");
    	}
    }
	
	public static String getThumbUrl(String fileName) {
		String simplify = fileName.replace(' ', '_');
		String md5 = DigestUtils.md5Hex(simplify);
		String hash1 = md5.substring(0, 1);
		String hash2 = md5.substring(0, 2);
		return ROOT_URL + "thumb/" + hash1 + "/" + hash2 + "/" + simplify + "/" + PREFIX + simplify;
	}

	public static String getUrl(String fileName) {
		String simplify = fileName.replace(' ', '_');
		String md5 = DigestUtils.md5Hex(simplify);
		String hash1 = md5.substring(0, 1);
		String hash2 = md5.substring(0, 2);
		return ROOT_URL + hash1 + "/" + hash2 + "/" + simplify;
	}
}
