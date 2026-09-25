package net.osmand.obf.preparation;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class UnglueNameTest {

	@Test
	public void letterBeforeWord() {
		check("L'abordage", "abordage");
		check("Au Soleil d'Austerlitz", "Au Soleil Austerlitz");
		check("Passage d'Étienne", "Passage Étienne");
		check("Carrer d'Andrea Dòria", "Carrer Andrea Dòria");
		check("Banca d'Italia", "Banca Italia");
		check("Eetcafé 't Pakhuis", "Eetcafé Pakhuis");
		check("O'Reilly Institute", "Reilly Institute");
		check("L’Hippocampe", "Hippocampe");
	}

	@Test
	public void letterAfterWord() {
		check("Garry's", "Garry");
		check("The Queen's Head", "The Queen Head");
		check("CARROLL'S IRISH PUB", "CARROLL IRISH PUB");
		check("Ben & Jerry's", "Ben & Jerry");
		check("Anke`s Pension", "Anke Pension");
		check("Umami’s Chinese Takeaway", "Umami Chinese Takeaway");
	}

	@Test
	public void lettersAroundWord() {
		check("O'Flaherty's", "Flaherty");
		check("L'Atelier d'Anaïs", "Atelier Anaïs");
		check("Rock'n'Roll Bar", "Rock Roll Bar");
		check("Pick 'N Roll Sport Pub", "Pick Roll Sport Pub");
	}

	@Test
	public void wordsGluedByApostrophe() {
		check("Chiesa di Sant'Ivo", "Chiesa di Sant Ivo");
		check("Teatro dell'Orologio", "Teatro dell Orologio");
		check("Women'secret", "Women secret");
		check("Meyer'sche Buchhandlung", "Meyer sche Buchhandlung");
		check("Saint-Leu-d'Esserent", "Saint-Leu-d Esserent");
	}

	@Test
	public void initials() {
		check("K.Nitz Grabmale", "Nitz Grabmale");
		check("E.T.A. Hoffmann", "Hoffmann");
		check("Soziokulturelles Zentrum e.V. MITTENDRIN", "Soziokulturelles Zentrum MITTENDRIN");
		check("П.И.Чайковский", "Чайковский");
		check("Будинок творчості А. Ерделі", "Будинок творчості Ерделі");
		check("ФАП с.Коноплівці", "ФАП Коноплівці");
		check("Wijkopenauto's.nl", "Wijkopenauto nl");
	}

	@Test
	public void onlyInitials() {
		check("A.P.C.", null);
		check("R.S.V.P.", null);
	}

	@Test
	public void apostropheInCyrillicWord() {
		check("Пам'ятник брехні і лавочка вибачення", null);
		check("Сім'я", null);
		check("В'їзд", null);
		check("Кав'ярня", null);
		check("пл. Лук'янівська", null);
	}

	@Test
	public void apostropheAtWordEdge() {
		check("Lovers' Walk", null);
		check("Dunkin' Donuts", null);
		check("Casa dell' acqua", null);
		check("Caffe' Verdi", null);
		check("'s-Gravesandeplein", null);
	}

	@Test
	public void abbreviationDot() {
		check("St. Josef", null);
		check("вул. 25-а Садова", null);
	}

	@Test
	public void wordWithDigits() {
		check("6178/2.Sokak", null);
		check("Monument Joods Verzet 1940-'45", null);
		check("Medaglie D'Oro 2", "Medaglie Oro 2");
	}

	@Test
	public void punctuationAroundWords() {
		check("Le Paris (l'Humour)", "Le Paris Humour");
		check("Colegio / Col·legi Santa Ana", null);
	}

	private static void check(String name, String expected) {
		assertEquals(name, expected, new AlternativeNameIndexGenerator.UnglueRule().alternativeName(name, null, null));
	}
}
