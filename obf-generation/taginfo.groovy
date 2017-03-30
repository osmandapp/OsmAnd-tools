def builder = new groovy.json.JsonBuilder()
def json = [:]
json["data_format"] = 1;
json["data_url"] = "http://builder.osmand.net:8080/view/WebSite/job/OsmAndTagInfo/ws/taginfo.json"

def txt = groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(json));
println txt
new File("taginfo.json").text = txt

