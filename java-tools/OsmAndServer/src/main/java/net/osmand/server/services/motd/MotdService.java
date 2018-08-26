package net.osmand.server.services.motd;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.codehaus.jackson.JsonParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;

@Service
public class MotdService {

    private final ObjectMapper mapper;

    @Value("${motd.settings}")
    private String motdSettings;

    @Autowired
    public MotdService(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public MotdSettings getSettings() throws IOException {
        return mapper.readValue(new File(motdSettings), MotdSettings.class);
    }
}
