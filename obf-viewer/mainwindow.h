#ifndef MAINWINDOW_H
#define MAINWINDOW_H

#include <QMainWindow>
#include <QSharedPointer>
#include <QFile>
#include <QErrorMessage>

#include <ObfReader.h>

namespace Ui {
class MainWindow;
}

class MainWindow : public QMainWindow
{
    Q_OBJECT
    
public:
    explicit MainWindow(QWidget *parent = 0);
    ~MainWindow();
    
private slots:
    void on_actionExit_triggered();
    void on_actionOpen_triggered();
    void on_actionClose_triggered();
    void on_poiSearchQuery_textEdited(const QString &arg1);
    void on_poiSearchQuery_editingFinished();

    void on_isPoiSearchLive_stateChanged(int arg1);

private:
    Ui::MainWindow *ui;
    QErrorMessage _error;
    QSharedPointer<QFile> _obfFile;
    QSharedPointer<OsmAnd::ObfReader> _obfReader;

    void loadCurrentObf();
    void unloadCurrentObf();

    // POI search tab
    void loadPoiSearch();
    void unloadPoiSearch();
};

#endif // MAINWINDOW_H
