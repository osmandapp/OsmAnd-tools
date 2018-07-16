package net.osmand.server.index.dao.zip;

import net.osmand.server.index.type.Type;
import net.osmand.server.index.type.TypeFactory;
import org.springframework.stereotype.Component;

@Component("Voices")
public class VoiceDAO extends ZipFilesDAO {

    private static final String VOICE_FILE_EXTENSION = "*.voice.zip";
    private static final String VOICE_PATH = "indexes";

    public VoiceDAO() {
        super(VOICE_PATH, VOICE_FILE_EXTENSION);
    }

    @Override
    public Type getType(TypeFactory typeFactory) {
        return typeFactory.newVoiceType();
    }
}
