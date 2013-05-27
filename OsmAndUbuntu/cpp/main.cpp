#include <QtGui/QGuiApplication>
#include <QGuiApplication>
#include <QQuickView>
#include <QtQml/qqmlcontext.h>
#include <stdio.h>
#include  "MainApplicationSettings.h"
#include "Map.h"
#include "MapLayersData.h"

int main(int argc, char *argv[])
{
    QGuiApplication* app = new QGuiApplication(argc, argv);
    QQuickView* view = new QQuickView;
    MainApplicationSettings* data = new MainApplicationSettings;
    Map* mdata= new Map;
    MapLayersData* ldata = new MapLayersData;

    //Resize Mode so the content of the QML file will scale to the window size
    view->setResizeMode(QQuickView::SizeRootObjectToView);

    //With this we can add the c++ Object to the QML file
    view->rootContext()->setContextProperty("applicationData", data);
    view->rootContext()->setContextProperty("mapData", mdata);
    view->rootContext()->setContextProperty("mapLayerData", ldata);

    //Resolve the relativ path to the absolute path (at runtime)
    const QString qmlFilePath= QString::fromLatin1("%1/%2").arg(QCoreApplication::applicationDirPath(), "qml/main.qml");
    view->setSource(QUrl::fromLocalFile(qmlFilePath));

    //Not sure if this is nessesary, but on mobile devices the app should start in fullscreen mode
    #if defined(Q_WS_SIMULATOR) || defined(Q_OS_QNX)
    view->showFullScreen();
    #else
    view->show();
    #endif
    int res = app->exec();
    delete view;


    delete data;
    delete mdata;
    delete ldata;
    delete app;

    return res;

}
