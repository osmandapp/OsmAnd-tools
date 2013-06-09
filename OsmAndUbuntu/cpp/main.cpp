#include <QtGui/QGuiApplication>
#include <QGuiApplication>
#include <QQuickView>
#include <QtQml/QQmlContext>
#include <QtQml/QQmlEngine>
#include <stdio.h>
#include <QObject>
#include "RootContext.h"

class QQuickImageProviderWrapper : public QQuickImageProvider {
public:
    QQuickImageProvider* base;
    QQuickImageProviderWrapper(QQuickImageProvider* base) : QQuickImageProvider(QQuickImageProvider::Image), base(base){}

    virtual QImage requestImage(const QString &id, QSize *size, const QSize &requestedSize) {
        return base->requestImage(id, size, requestedSize);
    }
};

int main(int argc, char *argv[])
{
    QGuiApplication* app = new QGuiApplication(argc, argv);
    QQuickView* view = new QQuickView;
    RootContext* r = new RootContext();
    //Resize Mode so the content of the QML file will scale to the window size
    view->setResizeMode(QQuickView::SizeRootObjectToView);

    //With this we can add the c++ Object to the QML file
    r->createProperties(view->rootContext());

    //Resolve the relativ path to the absolute path (at runtime)
    const QString qmlFilePath= QString::fromLatin1("%1/%2").arg(QCoreApplication::applicationDirPath(), "qml/main.qml");
    view->setSource(QUrl::fromLocalFile(qmlFilePath));
    view->engine()->addImageProvider("map", new QQuickImageProviderWrapper(r->getMapViewLayer()));
    //Not sure if this is nessesary, but on mobile devices the app should start in fullscreen mode
#if defined(Q_WS_SIMULATOR) || defined(Q_OS_QNX)
    view->showFullScreen();
#else
    view->show();
#endif
    int res = app->exec();
    view->engine()->removeImageProvider("map");
    delete view;
    delete r;
    return res;

}
