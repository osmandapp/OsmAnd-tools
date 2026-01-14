package net.osmand.swing;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.GradientPaint;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.RenderingHints;
import java.awt.Shape;
import java.awt.Stroke;
import java.awt.font.GlyphVector;
import java.awt.geom.AffineTransform;
import java.awt.geom.Line2D;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.osmand.data.DataTileManager;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.Way;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.ColorPalette;
import net.osmand.shared.routing.RouteColorize;
import net.osmand.shared.routing.RouteColorize.ColorizationType;
import net.osmand.shared.routing.RouteColorize.RouteColorizationPoint;
import net.osmand.util.MapUtils;

public class MapPointsLayer implements MapPanelLayer {

	private MapPanel map;

	// special points to draw
	private DataTileManager<Entity> points;


	private Color color = Color.black;
	private int size = 3;
	private String tagToShow = OSMTagKey.NAME.getValue();

	private Map<Point, Node> pointsToDraw = new LinkedHashMap<>();
	private List<LineObject> linesToDraw = new ArrayList<>();

	private Font whiteFont;
	GpxFile gpxFile;
	public ColorizationType colorizationType = ColorizationType.NONE;
	private boolean isGrey = true;

	private static class LineObject {
		Way w;
		Line2D line;
		boolean nameDraw;

		public LineObject(Way w, Line2D line, boolean nameDraw) {
			super();
			this.w = w;
			this.line = line;
			this.nameDraw = nameDraw;
		}

	}

	@Override
	public void destroyLayer() {
	}

	@Override
	public void initLayer(MapPanel map) {
		this.map = map;
	}

	public void setColor(Color color){
		this.color = color;
	}

	public void setPointSize(int size){
		this.size  = size;
	}

	public void setTagToShow(String tag) {
		this.tagToShow = tag;
	}


	@Override
	public void paintLayer(Graphics2D g) {
		drawLines(g);
		drawPoints(g);
	}

	private void drawLines(Graphics2D g) {
		List<LineObject> linesToDraw = this.linesToDraw;
		// draw user points
		int[] xPoints = new int[4];
		int[] yPoints = new int[4];
		boolean upDown = true;
		for (LineObject e : linesToDraw) {
			Line2D p = e.line;
			Way w = e.w;
			String name = null;
			boolean white = false;
			if (w != null) {
				if (e.nameDraw) {
					name = w.getTag("name");
				}
				white = "white".equalsIgnoreCase(w.getTag("color"));
				if (white) {
					g.setColor(Color.gray);
				} else if (w.getTag("colour") != null) {
					try {
						Color clr = (Color) Color.class.getField(w.getTag("colour").toUpperCase()).get(null);
						g.setColor(clr);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
				} else {
					g.setColor(color);
				}
			}
			AffineTransform transform = new AffineTransform();
			transform.translate(p.getX1(), p.getY1());
			transform.rotate(p.getX2() - p.getX1(), p.getY2() - p.getY1());
			xPoints[1] = xPoints[0] = 0;
			xPoints[2] = xPoints[3] = (int) Math.sqrt((p.getX2() - p.getX1())*(p.getX2() - p.getX1()) +
					(p.getY2() - p.getY1())*(p.getY2() - p.getY1())) +1;
			yPoints[3] = yPoints[0] = 0;
			yPoints[2] = yPoints[1] = 2;
			for (int i = 0; i < 4; i++) {
				Point2D po = transform.transform(new Point(xPoints[i], yPoints[i]), null);
				xPoints[i] = (int) po.getX();
				yPoints[i] = (int) po.getY();
			}
			g.drawPolygon(xPoints, yPoints, 4);
			g.fillPolygon(xPoints, yPoints, 4);
			if (name != null && name.length() > 0 && map.getZoom() >= 16) {
				Stroke prevStroke = g.getStroke();
				g.setStroke(new BasicStroke(3.0f));
				g.drawOval((int) p.getX2() - 6, (int) p.getY2() -6, 10, 10);

				Font prevFont = g.getFont();
				Color prevColor = g.getColor();
				AffineTransform prev = g.getTransform();

				double flt = Math.atan2(p.getX2() - p.getX1(), p.getY2() - p.getY1());

				AffineTransform ps = new AffineTransform(prev);
//				ps.translate((p.getX2() + p.getX1()) / 2, (int)(p.getY2() + p.getY1()) / 2);
				ps.translate(p.getX2(), p.getY2());
				if(flt < Math.PI && flt > 0) {
					ps.rotate(p.getX2() - p.getX1(), p.getY2() - p.getY1());
				} else {
					ps.rotate(-(p.getX2() - p.getX1()), -(p.getY2() - p.getY1()));
				}
				g.setTransform(ps);
				GlyphVector gv = whiteFont.createGlyphVector(g.getFontRenderContext(), name);
				int shiftX = (int) (gv.getVisualBounds().getWidth() / 2);
				int shiftY = upDown ? 15 : -15;
				upDown = !upDown;
				Shape textShape = gv.getOutline(-shiftX, -shiftY);
				
				g.setColor(Color.white);
				g.setFont(whiteFont);
//				g.drawString(name, -15, -10);
				g.draw(textShape);

				if(white) {
					g.setColor(Color.lightGray);
				} else {
					g.setColor(Color.DARK_GRAY);
				}
				g.drawString(name, -shiftX, -shiftY);
				
				g.setStroke(prevStroke);
				g.setColor(prevColor);
				g.setTransform(prev);
				g.setFont(prevFont);
			}
		}
	}

	private void drawPoints(Graphics2D g) {
		Map<Point, Node> pointsToDraw = this.pointsToDraw;
		g.setColor(color);
		if (whiteFont == null) {
			whiteFont = g.getFont().deriveFont(21).deriveFont(Font.BOLD);
		}
		g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
		g.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
		// draw user points
		for (Point p : pointsToDraw.keySet()) {
			Node n = pointsToDraw.get(p);
			if (n.getTag("colour") != null) {
				try {
					Color clr = (Color) Color.class.getField(n.getTag("colour").toUpperCase()).get(null);
					g.setColor(clr);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			} else {
				g.setColor(color);
			}
			g.drawOval(p.x, p.y, size, size);
			g.fillOval(p.x, p.y, size, size);
			boolean showGpxPointNumber = !isGpx(n) || (DataExtractionSettings.getSettings().isAnimateRouting()
					&& map.getZoom() >= 18);
			if (tagToShow != null && n.getTag(tagToShow) != null && showGpxPointNumber && map.getZoom() > 14) {
				int i = 0;
				int k;
				String s = n.getTag(tagToShow);
				while ((k = s.indexOf('\n')) != -1) {
					g.drawString(s.substring(0, k), p.x, (p.y + i++ * 15));
					s = s.substring(k + 1);
				}
				g.drawString(s, p.x, (p.y + i++ * 15));
			}
		}
		if (colorizationType != ColorizationType.NONE) {
			colorizeRoute(g);
		}
	}

	private boolean isGpx(Node n) {
		return n.getTag("gpx") != null;
	}

	@Override
	public void prepareToDraw() {
		if (points != null) {
			Map<Point, Node> pointsToDraw = new LinkedHashMap<>();
			List<LineObject> linesToDraw = new ArrayList<>();
			double xTileLeft = map.getXTile() - map.getCenterPointX() / map.getTileSize();
			double xTileRight = map.getXTile() + map.getCenterPointX() / map.getTileSize();
			double yTileUp = map.getYTile() - map.getCenterPointY() / map.getTileSize();
			double yTileDown = map.getYTile() + map.getCenterPointY() / map.getTileSize();

			double latDown = MapUtils.getLatitudeFromTile(map.getZoom(), yTileDown);
			double longDown = MapUtils.getLongitudeFromTile(map.getZoom(), xTileRight);
			double latUp = MapUtils.getLatitudeFromTile(map.getZoom(), yTileUp);
			double longUp = MapUtils.getLongitudeFromTile(map.getZoom(), xTileLeft);
			List<? extends Entity> objects = points.getObjects(latUp, longUp, latDown, longDown);
			pointsToDraw.clear();
			linesToDraw.clear();
			for (Entity e : objects) {
				if (e instanceof Way) {
					List<Node> nodes = ((Way) e).getNodes();
					if (nodes.size() > 1) {
						int prevPixX = 0;
						int prevPixY = 0;
						for (int i = 0; i < nodes.size(); i++) {
							Node n = nodes.get(i);
							int pixX = (int) (MapUtils.getPixelShiftX(map.getZoom(), n.getLongitude(),
									map.getLongitude(), map.getTileSize()) + map.getCenterPointX());
							int pixY = (int) (MapUtils.getPixelShiftY(map.getZoom(), n.getLatitude(), map.getLatitude(),
									map.getTileSize()) + map.getCenterPointY());
							if (i > 0) {
								// i == nodes.size() / 2
								
								linesToDraw.add(new LineObject((Way) e,
										new Line2D.Float(pixX, pixY, prevPixX, prevPixY), i == 1));

							}
							prevPixX = pixX;
							prevPixY = pixY;
						}
					}

				} else if(e instanceof Node){
					Node n = (Node) e;
					int pixX = (int) (MapUtils.getPixelShiftX(map.getZoom(), n.getLongitude(), map.getLongitude(), map.getTileSize()) + map.getCenterPointX());
					int pixY = (int) (MapUtils.getPixelShiftY(map.getZoom(), n.getLatitude(), map.getLatitude(), map.getTileSize()) + map.getCenterPointY());
					if (pixX >= 0 && pixY >= 0) {
						pointsToDraw.put(new Point(pixX, pixY), n);
					}
				} else {
				}
			}
			this.linesToDraw = linesToDraw;
			this.pointsToDraw = pointsToDraw;
		}
	}

	public DataTileManager<Entity> getPoints() {
		return points;
	}

	public void setPoints(DataTileManager<Entity> points) {
		this.points = points;
	}

	public void setColorizationType(GpxFile gpxFile, ColorizationType colorizationType, boolean grey) {
		this.gpxFile = gpxFile;
		this.colorizationType = colorizationType;
		this.isGrey = grey;
		
	}

	private void colorizeRoute(Graphics2D g) {
		if (this.gpxFile == null || colorizationType == ColorizationType.NONE) {
			return;
		}

		
		ColorPalette palette;
		if (isGrey) {
			palette = ColorPalette.Companion.parsePalette(new double[][]
					{ { 0, ColorPalette.Companion.getLIGHT_GREY() }, { 1, ColorPalette.Companion.getDARK_GREY() } });
		} else if (colorizationType == ColorizationType.SLOPE) {
			palette = ColorPalette.Companion.getSLOPE_PALETTE();
		} else {
			palette = ColorPalette.Companion.getMIN_MAX_PALETTE();
		}
		RouteColorize routeColorize = new RouteColorize(gpxFile, colorizationType, palette);
		//double[][] palette = {{routeColorize.minValue, RouteColorize.YELLOW}, {routeColorize.maxValue, RouteColorize.RED}};
		//double[][] palette = {{routeColorize.minValue,46,185,0,191}, {(routeColorize.maxValue + routeColorize.minValue) / 2, RouteColorize.YELLOW}, {routeColorize.maxValue, RouteColorize.RED}};
		routeColorize.setPalette(palette);
		List<RouteColorizationPoint> dataList = routeColorize.getSimplifiedResult(map.getZoom());

		for (int i = 1; i < dataList.size(); i++) {
			int pixX1 = (int) (MapUtils.getPixelShiftX(map.getZoom(), dataList.get(i - 1).getLon(), map.getLongitude(), map.getTileSize()) + map.getCenterPointX());
			int pixY1 = (int) (MapUtils.getPixelShiftY(map.getZoom(), dataList.get(i - 1).getLat(), map.getLatitude(), map.getTileSize()) + map.getCenterPointY());
			int pixX2 = (int) (MapUtils.getPixelShiftX(map.getZoom(), dataList.get(i).getLon(), map.getLongitude(), map.getTileSize()) + map.getCenterPointX());
			int pixY2 = (int) (MapUtils.getPixelShiftY(map.getZoom(), dataList.get(i).getLat(), map.getLatitude(), map.getTileSize()) + map.getCenterPointY());
			GradientPaint gp = new GradientPaint(pixX1, pixY1, new Color(dataList.get(i - 1).getPrimaryColor()), pixX2, pixY2,
					new Color(dataList.get(i).getPrimaryColor()), false);
			g.setPaint(gp);
			g.setStroke(new BasicStroke(10));
			g.draw(new Line2D.Float(pixX1, pixY1, pixX2, pixY2));
		}
	}

	@Override
	public void applySettings() {
	}

}
