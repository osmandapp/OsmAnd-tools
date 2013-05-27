#include <QtGui/QGuiApplication>
#include <QGuiApplication>
#include <QQuickView>
#include <QtQml/qqmlcontext.h>
#include <stdio.h>
#include <QObject>
#include "RootContext.h"


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

    //Not sure if this is nessesary, but on mobile devices the app should start in fullscreen mode
    #if defined(Q_WS_SIMULATOR) || defined(Q_OS_QNX)
    view->showFullScreen();
    #else
    view->show();
    #endif
    int res = app->exec();
    delete view;
    delete r;
    return res;

}
