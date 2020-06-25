package net.osmand.swing;

import java.awt.Graphics2D;

import javax.swing.JPopupMenu;

public interface MapPanelLayer {

	public void initLayer(MapPanel map);

	public void destroyLayer();
	
	default public void fillPopupMenuWithActions(JPopupMenu jPopupMenu) {};

	public void prepareToDraw();

	public void paintLayer(Graphics2D g);

	public void applySettings();

}
