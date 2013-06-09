#include "MapViewLayer.h"
#include <QPainter>
#include <qmath.h>

MapViewLayer::MapViewLayer(MapViewAdapter* adapter, QObject *parent) : QQuickImageProvider(QQuickImageProvider::Image),
    adapter(adapter), QObject(parent), app(OsmAnd::OsmAndApplication::getAndInitializeApplication()),
    rasterLayer(new OsmAnd::OsmAndRasterMapLayer(app))
{
    adapter->getMapView()->addLayer(rasterLayer);
}


QImage MapViewLayer::requestImage(const QString &id, QSize *size, const QSize &requestedSize) {
    OsmAnd::MapPoint tl, br;
    SkBitmap* bmp = rasterLayer->getBitmap(&tl, &br);
    if(bmp != nullptr) {
        if(size) {
            size->setWidth(bmp->width());
            size->setHeight(bmp->height());
        }
        ids[id] = std::pair<OsmAnd::MapPoint, OsmAnd::MapPoint>(tl, br);
        QImage mg = QImage((uchar*)bmp->getPixels(), bmp->width(), bmp->height(), bmp->rowBytes(), QImage::Format_ARGB32);
        return mg;
    }
    return QImage();
}
