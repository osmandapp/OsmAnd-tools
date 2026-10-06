package net.osmand.obf.preparation;

import net.osmand.binary.CommonWordsMultiIndex;

/**
 * Why a word of a name or of an alternative name is or is not a key of the name index (rules-spec.md, 4.2): the
 * outcomes are counted per name index and per rule and written to the log of the OBF writer.
 */
public enum KeyDecision {
	// a key by the statistics of the group (or the map has no group)
	KEPT(true),
	// a key: the object is notable, the statistics do not apply
	NOTABLE(true),
	// not a key: class 1 and the name has a word outside class 1
	DROPPED_CLASS1(false),
	// not a key: class 2 and the name has a word at least RARER_FACTOR times rarer
	DROPPED_CLASS2(false),
	// a number or a marker of the index: the statistics do not apply
	NUMBER(false),
	// a key: a new word of an alternative name kept by its own class
	ALT(true),
	// not a key: a new word of an alternative name dropped by its own class, a word of the name under the keys of the name
	ALT_ATTACHED(false),
	// not a key: a word of an alternative name that the name has and does not index
	ALT_SHADOWED(false),
	// a key: <class0> of the rules or a new word of an alternative name of a rule with keys="always"
	ALWAYS(true),
	// a key: a word dropped by the statistics that the legacy search picks (SearchPhrase)
	LEGACY(true);

	public final boolean key;

	KeyDecision(boolean key) {
		this.key = key;
	}

	static KeyDecision of(CommonWordsMultiIndex.KeyOutcome outcome) {
		return switch (outcome) {
			case KEPT -> KEPT;
			case NOTABLE -> NOTABLE;
			case ALWAYS -> ALWAYS;
			// a number with letters ("22a", "33-я") is a key; a pure number is decided by the writer
			case NUMBER -> KEPT;
			case DROPPED_CLASS1 -> DROPPED_CLASS1;
			case DROPPED_CLASS2 -> DROPPED_CLASS2;
		};
	}
}
