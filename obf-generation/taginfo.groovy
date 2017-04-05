def processType(tp, uniqueset,tags) {
	def tg = tp."@tag".text();
	def value = tp."@value".text();
	if ( uniqueset[tg + "=" + value] )  return ;
	uniqueset[tg + "=" + value] = tg;
	def notosm = tp."@notosm".text();
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
def processPOItype(tp, uniqueset,tags) {
	def tg = tp."@tag".text();
	def value = tp."@value".text();
	if ( uniqueset[tg + "=" + value] )  return ;
	uniqueset[tg + "=" + value] = tg;
	def notosm = tp."@notosm".text();
	if(!tg.contains("osmand") && value != "" && notosm != "true") {
		def taginfop = [:]
		taginfop["key"] = tg;
		if(value != "") {
			taginfop["value"] = value;
		}
		taginfop["description"] = "Used to create maps (POI)";
		tags << taginfop
	}	
}


def builder = new groovy.json.JsonBuilder()



def json = [:]
json["data_format"] = 1;
json["data_url"] = "http://builder.osmand.net:8080/view/WebSite/job/OsmAndTagInfo/ws/taginfo.json"
json["data_updated"] = new Date().format("yyyyMMdd'T'hhmmssZ") 
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

poiTypes.poi_type.each { tp ->
	tp.poi_additional_category.each { pac ->
		pac.poi_additional.each { pa ->
			processPOItype(pa, uniqueset, tags)	
		}
	}
	tp.poi_additional.each { pa ->
		processPOItype(pa, uniqueset, tags)	
	}
}
poiTypes.poi_additional.each { pa ->
	processPOItype(pa, uniqueset, tags)	
}
/*
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
*/
json["tags"] = tags
def txt = groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(json));
println txt
new File("taginfo.json").text = txt

