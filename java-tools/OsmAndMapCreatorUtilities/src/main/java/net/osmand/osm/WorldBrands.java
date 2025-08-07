package net.osmand.osm;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class WorldBrands {

    private List<String> worldBrands;

    public WorldBrands() {
        try {
            init();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void init() throws IOException {
        worldBrands = new ArrayList<>();
        InputStream is = WorldBrands.class.getResourceAsStream("world_brands.csv");
        if (is == null) {
            throw new IOException("world_brands.csv was not found");
        }
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(" ");
                String brand = "";
                boolean firstNum = true;
                for (String v : values) {
                    if (!v.isEmpty() && firstNum) {
                        firstNum = false;
                        continue;
                    }
                    brand = brand.isEmpty() ? v : brand + " " + v;
                }
                worldBrands.add(brand.toLowerCase());
            }
        }
    }

    public boolean isWorldBrand(String brand) {
        return worldBrands.contains(brand.toLowerCase());
    }
}
