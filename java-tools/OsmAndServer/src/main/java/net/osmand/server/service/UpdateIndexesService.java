package net.osmand.server.service;

import net.osmand.server.index.dao.*;
import net.osmand.server.index.type.Type;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
public class UpdateIndexesService implements UpdateIndexes {

    @Override
    public void update() {
        AbstractDAO mapsDAO = new MapsDAO();
        AbstractDAO voicesDAO = new VoiceDAO();
        AbstractDAO fontsDAO = new FontsDAO();
        AbstractDAO depthsDAO = new DepthDAO();
        try {
            printAll(mapsDAO.getAll());
            printAll(voicesDAO.getAll());
            printAll(fontsDAO.getAll());
            printAll(depthsDAO.getAll());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void printAll(List<Type> types) {
        for (Type type : types) {
            System.out.println(" Element name : " + type.getElementName());
            System.out.println("         type : " + type.getType());
            System.out.println("containerSize : " + type.getContainerSize());
            System.out.println("  contentSize : " + type.getContentSize());
            System.out.println("    timestamp : " + type.getTimestamp());
            System.out.println("         date : " + type.getDate());
            System.out.println("         size : " + type.getSize());
            System.out.println("   targetSize : " + type.getTargetSize());
            System.out.println("         name : " + type.getName());
            System.out.println("  description : " + type.getDescription());
            System.out.println("--------------------------------------");

        }
    }
}
