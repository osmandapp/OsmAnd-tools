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
def uniqueset = [:]
renderingTypes.type.each { tp ->
	def tg = tp."@tag".text();
	def value = tp."@value".text();
	if ( uniqueset[tg + "=" + value] )  return ;
	uniqueset[tg + "=" + value] = tg;
	def notosm = tp."@notosm".text();
	if(!tg.contains("osmand") && value != "" && notosm != "true") {
		def taginfop = [:]
		taginfop["key"] = tg;
		taginfop["value"] = value;
		taginfop["description"] = "used to create maps";
		tags << taginfop
	}	
}
renderingTypes.entity_convert.each { tp ->
	def tg = tp."@from_tag".text();
	def value = tp."@from_value".text();
	if ( uniqueset[tg + "=" + value] )  return ;
	uniqueset[tg + "=" + value] = tg;
	def notosm = tp."@notosm".text();
	if(!tg.contains("osmand") && notosm != "true") {
		def taginfop = [:]
		taginfop["key"] = tg;
		taginfop["value"] = value;
		taginfop["description"] = "used to create maps";
		tags << taginfop
	}	
}
json["tags"] = tags
def txt = groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(json));
println txt
new File("taginfo.json").text = txt

