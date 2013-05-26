#include <QtGui/QGuiApplication>
#include <QGuiApplication>
#include <QQuickView>
#include <QtQml/qqmlcontext.h>
#include <stdio.h>
#include  "MainApplicationSettings.h"
#include "Map.h"

int main(int argc, char *argv[])
{
    QGuiApplication app(argc, argv);
    QQuickView view;
    MainApplicationSettings data;
    Map mdata;

    //Resize Mode so the content of the QML file will scale to the window size
    view.setResizeMode(QQuickView::SizeRootObjectToView);

    //With this we can add the c++ Object to the QML file
    view.rootContext()->setContextProperty("applicationData", &data);
    view.rootContext()->setContextProperty("mapData", &mdata);

    //Resolve the relativ path to the absolute path (at runtime)
    const QString qmlFilePath= QString::fromLatin1("%1/%2").arg(QCoreApplication::applicationDirPath(), "qml/main.qml");
    view.setSource(QUrl::fromLocalFile(qmlFilePath));

    //Not sure if this is nessesary, but on mobile devices the app should start in fullscreen mode
    #if defined(Q_WS_SIMULATOR) || defined(Q_OS_QNX)
    view.showFullScreen();
    #else
    view.show();
    #endif
    return app.exec();
}
