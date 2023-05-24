def processType(tp, uniqueset,tags) {
	def tg = tp."@tag".text();
	def value = tp."@value".text();
	if ( uniqueset[tg + "=" + value] )  return ;
	uniqueset[tg + "=" + value] = tg;
	def notosm = tp."@no_edit".text();
	if(!tg.contains("osmand") && value != "" && notosm != "true") {
		def taginfop = [:]
		taginfop["key"] = tg;
		taginfop["value"] = value;
		taginfop["description"] = "Used to create maps";
		tags << taginfop
	}	
}
def processEntityConvert(tp, uniqueset, tags) {
	def tg = tp."@from_tag".text();
	def value = tp."@from_value".text();
	if ( uniqueset[tg + "=" + value] )  return ;
	uniqueset[tg + "=" + value] = tg;
	def notosm = tp."@notosm".text();
	if(!tg.contains("osmand") && notosm != "true") {
		def taginfop = [:]
		taginfop["key"] = tg;
		if(value != "") {
			taginfop["value"] = value;
		}
		taginfop["description"] = "Used to create maps";
		tags << taginfop
	}	
}

DEFAULT_HTTP_URL = "https://raw.githubusercontent.com/osmandapp/OsmAnd-resources/master/rendering_styles/style-icons/drawable-hdpi/";

def processPOItype(tp, uniqueset,tags) {
	def tg = tp."@tag".text();
	def value = tp."@value".text();
	def name = tp."@name".text();
	if ( uniqueset[tg + "=" + value] )  return ;
	uniqueset[tg + "=" + value] = tg;
	def notosm = tp."@no_edit".text();
	if(!tg.contains("osmand") && notosm != "true") {
		def taginfop = [:]
		taginfop["key"] = tg;
		if(value != "") {
			taginfop["value"] = value;
		}
		taginfop["description"] = "Used to create maps (POI)";
		String folder = "resources/rendering_styles/style-icons/drawable-hdpi/";
		if(new File(folder, "mx_" + name + ".png").exists()) {
			taginfop["icon_url"] =  DEFAULT_HTTP_URL + "mx_" + name + ".png";
		} else if(new File(folder, "mx_" + tg + "_" + value + ".png").exists()) {
			taginfop["icon_url"] =  DEFAULT_HTTP_URL + "mx_" + tg + "_" + value  + ".png";
		} else if(new File(folder, "mx_" + value + ".png").exists()) {
			taginfop["icon_url"] =  DEFAULT_HTTP_URL + "mx_" + value  + ".png";
		}
		
		tags << taginfop
	}	
}
def processPOIGroup(group, uniqueset,tags) {
	group.poi_type.each { tp ->
		processPOItype(tp, uniqueset, tags)	
		processPOIGroup(tp, uniqueset, tags)
	}
	group.poi_additional.each { pa ->
		processPOItype(pa, uniqueset, tags)	
	}
	group.poi_category.each { pac ->
		processPOIGroup(pac, uniqueset, tags)
	}
	group.poi_filter.each { pf ->
		processPOIGroup(pf, uniqueset, tags)
	}
	group.poi_additional_category.each { pac ->
		processPOIGroup(pac, uniqueset, tags)
	}

}


def builder = new groovy.json.JsonBuilder()



def json = [:]
json["data_format"] = 1;
json["data_url"] = "https://creator.osmand.net/taginfo.json"
json["data_updated"] = new Date().format("yyyyMMdd'T'HHmmss'Z'") 
json["project"] = [
	"name": "OsmAnd",
	"description": "OsmAnd Maps & Navigation",
	"project_url": "http://osmand.net",
	"icon_url": "https://raw.githubusercontent.com/osmandapp/OsmAnd-misc/master/logo/simple/osmand-app-72.png",
	"contact_name": "OsmAnd Team",
	"contact_email": "contactus@osmand.net"
];
def tags = []

def renderingTypes = new XmlSlurper().parse("resources/obf_creation/rendering_types.xml")
def poiTypes = new XmlSlurper().parse("resources/poi/poi_types.xml")
def uniqueset = [:]

processPOIGroup(poiTypes, uniqueset, tags);
renderingTypes.type.each { tp ->
	processType(tp, uniqueset, tags)
}
renderingTypes.entity_convert.each { tp ->
	processEntityConvert(tp, uniqueset, tags)	
}
renderingTypes.category.each { c ->
	c.type.each { tp ->
		processType(tp, uniqueset, tags)
	}
	c.entity_convert.each { tp ->
		processEntityConvert(tp, uniqueset, tags)	
	}
}

json["tags"] = tags
def txt = groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(json));
println txt
new File("taginfo.json").text = txt

