package net.osmand.search;

import java.io.IOException;
import java.util.List;

public interface SearchEngine {
    List<String> apply(String text, List<String> expectedResults) throws IOException;
    
    void close();
}
