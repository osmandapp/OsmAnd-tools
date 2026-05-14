package net.osmand.reviews.mangrove;

import com.google.common.collect.ImmutableMap;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * An <a href="https://datatracker.ietf.org/doc/rfc5870/">RFC 5870</a> Geo URI representation.
 * Only supports features used by Mangrove.
 */
public record GeoUri(double coordA, double coordB, Double uncertainty, Map<String, String> parameters) {
    public static GeoUri parseUrlEncoded(String uriString) {
        String[] schemeAndRest = uriString.split(":", 2);
        String scheme = schemeAndRest[0];
        String rest = schemeAndRest[1];
        if (!"geo".equals(scheme)) {
            throw new IllegalArgumentException(String.format("expected scheme 'geo', got '%s' in '%s'", scheme, uriString));
        }
        String[] coordsAndParams = rest.split("\\?");
        String coordsStr = coordsAndParams[0];
        String paramsStr = coordsAndParams[1];
        String[] coordsStrs = coordsStr.split(",");
        String[] nameValuePairs = paramsStr.split("&");
        ImmutableMap.Builder<String, String> params = ImmutableMap.builder();
        Double uncertainty = null;
        for (String pairStr : nameValuePairs) {
            String[] nameAndValue = pairStr.split("=");
            String name = nameAndValue[0];
            String value = nameAndValue[1];
            if (name.equals("u")) {
                uncertainty = Double.parseDouble(value);
            } else {
                params.put(name, URLDecoder.decode(value, StandardCharsets.UTF_8));
            }
        }
        return new GeoUri(Double.parseDouble(coordsStrs[0]), Double.parseDouble(coordsStrs[1]), uncertainty, params.build());
    }

    public double latitude() {
        return coordA;
    }

    public double longitude() {
        return coordB;
    }
}
