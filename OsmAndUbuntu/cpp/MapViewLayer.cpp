#include "MapViewLayer.h"
#include <QPainter>
#include <qmath.h>

MapViewLayer::MapViewLayer(MapViewAdapter* adapter, QObject *parent) : QQuickImageProvider(QQuickImageProvider::Pixmap),
    adapter(adapter), QObject(parent), img(nullptr), app(OsmAnd::OsmAndApplication::getAndInitializeApplication())
{
}

const QImage* MapViewLayer::loadImage(QString &s) {
    auto i = cache.find(s);
    if(i == cache.end()) {
        return  (cache[s] = new QImage(s));
    } else {
        return *i;
    }

}

QPixmap MapViewLayer::requestPixmap(const QString &id, QSize *size, const QSize& requestedSize) {
//    if(img == nullptr || (img->size().width() != adapter->getWidth() || adapter->getHeight() != img->size().height())) {
        if(img != nullptr) {
            delete img;
        }
        img = new QPixmap(QSize(adapter->getWidth(), adapter->getHeight()));
  //  }
    QRectF ts =  adapter->getTiles();
    int left =floor(ts.x());
    int top = floor(ts.y());
    int width  = ceil(ts.x() + ts.width()) - left;
    int height  = ceil(ts.y() + ts.height()) - top;

    float tileX = adapter->getXTile();
    float tileY = adapter->getYTile();
    float w = adapter->getCenterPointX();
    float h = adapter->getCenterPointY();
    float ftileSize = adapter->getTileSize();
    QString base = app->getSettings()->APPLICATION_DIRECTORY.get().toString() + "/tiles/Mapnik/";
    base = base + QString::number(adapter->getZoom())+"/";

    QVector<QVector<const QImage*> > images;
    images.resize(width);
    for (int i = 0; i < width; i++) {
        images[i].resize(height);
        for (int j = 0; j < height; j++) {
            QString s = base + QString::number(left + i) +"/"+QString::number(top+j)+".png.tile";
            images[i][j] = loadImage(s);
        }
    }
    QPainter p(img);
    for (int  i = 0; i < images.size(); i++) {
        for (int j = 0; j < images[i].size(); j++) {
            float x1 = (left + i - tileX) * ftileSize + w;
            float y1 = (top + j - tileY) * ftileSize + h;
            p.drawImage(x1, y1, *images[i][j], 0, 0,ftileSize, ftileSize);
        }
    }
    if(size) {
        size->setWidth(img->width());
        size->setHeight(img->height());
    }

    return *img;
}
