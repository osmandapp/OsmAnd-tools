package net.osmand.data.preparation.address;

import java.util.LinkedHashMap;
import java.util.Map;

public class CommonWords {
	private static Map<String, Integer> commonWordsDictionary = new LinkedHashMap<>();
	private static void addCommon(String string) {
		commonWordsDictionary.put(string, commonWordsDictionary.size());
	}
	
	public static int getCommon(String name) {
		Integer i = commonWordsDictionary.get(name);
		return i == null ? -1 : i.intValue();
	}
	
	static {
		addCommon("la");
		addCommon("via");
		addCommon("rua");
		addCommon("de");
		addCommon("du");
		addCommon("des");
		addCommon("del");
		addCommon("am");
		addCommon("da");
		addCommon("a");
		addCommon("der");
		addCommon("do");
		addCommon("los");
		addCommon("di");
		addCommon("im");
		addCommon("el");
		addCommon("e");
		addCommon("an");
		addCommon("g.");
		addCommon("rd");
		addCommon("dos");
		addCommon("dei");
		addCommon("b");
		addCommon("st");
		addCommon("the");
		addCommon("las");
		addCommon("f");
		addCommon("u");
		addCommon("jl.");
		addCommon("j");
		addCommon("sk");
		addCommon("w");
		addCommon("a.");
		addCommon("of");
		addCommon("k");
		addCommon("r");
		addCommon("h");
		addCommon("mc");
		addCommon("sw");
		addCommon("g");
		addCommon("v");
		addCommon("m");
		addCommon("c.");
		addCommon("r.");
		addCommon("ct");
		addCommon("e.");
		addCommon("dr.");
		addCommon("j.");
		addCommon("in");
		addCommon("al");
		addCommon("út");
		addCommon("per");
		addCommon("ne");
		addCommon("p");
		addCommon("et");
		addCommon("s.");
		addCommon("f.");
		addCommon("t");
		addCommon("fe");
		addCommon("à");
		addCommon("i");
		addCommon("c");
		addCommon("le");
		addCommon("s");
		addCommon("av.");
		addCommon("den");
		addCommon("dr");
		addCommon("y");
		
		
		
		addCommon("van");
		addCommon("road");
		addCommon("street");
		addCommon("drive");
		addCommon("avenue");
		addCommon("rue");
		addCommon("lane");
		addCommon("улица");
		addCommon("спуск");
		addCommon("straße");
		addCommon("chemin");
		addCommon("way");

		addCommon("court");
		addCommon("calle");

		addCommon("place");

		addCommon("avenida");
		addCommon("boulevard");
		addCommon("county");
		addCommon("route");
		addCommon("trail");
		addCommon("circle");
		addCommon("close");
		addCommon("highway");
		
		addCommon("strada");
		addCommon("impasse");
		addCommon("utca");
		addCommon("creek");
		addCommon("carrer");
		addCommon("вулиця");
		addCommon("allée");
		addCommon("weg");
		addCommon("площадь");
		addCommon("тупик");

		addCommon("terrace");
		addCommon("jalan");
		
		addCommon("parkway");
		addCommon("переулок");
		
		addCommon("carretera");
		addCommon("valley");
		
		addCommon("camino");
		addCommon("viale");
		addCommon("loop");
		
		addCommon("bridge");
		addCommon("embankment");
		addCommon("township");
		addCommon("town");
		addCommon("village");
		addCommon("piazza");
		addCommon("della");
		
		addCommon("plaza");
		addCommon("pasaje");
		addCommon("expressway");
		addCommon("ruta");
		addCommon("square");
		addCommon("freeway");
		addCommon("line");
		
		addCommon("track");
		
		addCommon("zum");
		addCommon("rodovia");
		addCommon("sokak");
		addCommon("sur");
		addCommon("path");
		addCommon("das");
		
		addCommon("yolu");
		
		addCommon("проспект");

		addCommon("auf");
		addCommon("alley");
		addCommon("são");
		addCommon("les");
		addCommon("delle");
		addCommon("paseo");
		addCommon("alte");
		addCommon("autostrada");
		addCommon("iela");
		addCommon("autovía");
		addCommon("d");
		addCommon("ulica");
		
		addCommon("na");
		addCommon("проезд");
		addCommon("n");
		addCommon("ул.");
		addCommon("voie");
		addCommon("ring");
		addCommon("ruelle");
		addCommon("vicolo");
		addCommon("avinguda");
		addCommon("шоссе");
		addCommon("zur");
		addCommon("corso");
		addCommon("autopista");
		addCommon("провулок");
		addCommon("broadway");
		addCommon("to");
		addCommon("passage");
		addCommon("sentier");
		addCommon("aleja");
		addCommon("dem");
		addCommon("valle");
		addCommon("cruz");

		addCommon("bypass");
		addCommon("rúa");
		addCommon("crest");
		addCommon("ave");
		
		addCommon("expressway)");
		
		addCommon("autoroute");
		addCommon("crossing");
		addCommon("camí");
		addCommon("bend");
		
		addCommon("end");
		addCommon("caddesi");
		addCommon("bis");
		
		addCommon("ქუჩა");
		addCommon("kalea");
		addCommon("pass");
//		addCommon("ponte");
		addCommon("cruce");
		addCommon("se");
		addCommon("au");

		addCommon("allee");
		addCommon("autobahn");
		addCommon("väg");
		addCommon("sentiero");
		addCommon("plaça");
		addCommon("o");
		addCommon("vej");
		addCommon("aux");
		addCommon("spur");
		addCommon("ringstraße");
		addCommon("prospect");
		addCommon("m.");
		addCommon("chaussee");
		addCommon("row");
		addCommon("link");
	
		addCommon("travesía");
		addCommon("degli");
		addCommon("piazzale");
		addCommon("vei");
		addCommon("waldstraße");
		addCommon("promenade");
		addCommon("puente");
		addCommon("rond-point");
		addCommon("vía");
		addCommon("pod");
		addCommon("triq");
		addCommon("hwy");
		addCommon("οδός");
		addCommon("dels");
		addCommon("and");

		addCommon("pré");
		addCommon("plac");
		addCommon("fairway");
	
// 		addCommon("farm-to-market");

		addCommon("набережная");

		addCommon("chaussée");

		addCommon("náměstí");
		addCommon("tér");
		addCommon("roundabout");
		addCommon("lakeshore");
		addCommon("lakeside");
		addCommon("alle");
		addCommon("gasse");
		addCommon("str.");
//		addCommon("p.");
		addCommon("ville");
		addCommon("beco");
		addCommon("platz");

// 		addCommon("porto");

		addCommon("sideroad");
		addCommon("pista");

		addCommon("аллея");
		addCommon("бульвар");
		addCommon("город");
		addCommon("городок");
		addCommon("деревня");
		addCommon("дер.");
		addCommon("пос.");
		addCommon("дорога");
		addCommon("дорожка");
		addCommon("кольцо");
		addCommon("мост");
		addCommon("остров");
		addCommon("островок");
		addCommon("поселок");
		addCommon("посёлок");
		addCommon("путепровод");
		addCommon("слобода");
		addCommon("станция");
		addCommon("тоннель");
		addCommon("тракт");
		addCommon("island");
		addCommon("islet");
		addCommon("tunnel");
		addCommon("stadt");
		addCommon("brücke");
		addCommon("damm");
		addCommon("insel");
		addCommon("dorf");
		addCommon("bereich");
		addCommon("überführung");
		addCommon("bulevar");
		addCommon("ciudad");
		addCommon("pueblo");
		addCommon("anillo");
		addCommon("muelle");
		addCommon("isla");
		addCommon("islote");
		addCommon("carril");
		addCommon("viaje");

	}


	
}
