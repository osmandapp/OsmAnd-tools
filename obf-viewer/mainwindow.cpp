#include "mainwindow.h"
#include "ui_mainwindow.h"

#include <QFileDialog>

MainWindow::MainWindow(QWidget *parent)
 : QMainWindow(parent)
 , ui(new Ui::MainWindow)
 , _error(this)
{
    ui->setupUi(this);
    unloadCurrentObf();
}

MainWindow::~MainWindow()
{
    delete ui;
}

void MainWindow::on_actionOpen_triggered()
{
    auto fileName = QFileDialog::getOpenFileName(this,
        tr("Open File"), "", tr("OsmAnd binary files (*.obf)"));

    // Try to open new file
    QSharedPointer<QFile> obfFile(new QFile(fileName));
    if(!obfFile->exists())
    {
        _error.showMessage("File [" + fileName + "] does not exist!");
        return;
    }
    if(!obfFile->open(QIODevice::ReadOnly))
    {
        _error.showMessage("Failed to open file [" + fileName + "]!");
        return;
    }
    QSharedPointer<OsmAnd::ObfReader> obfReader(new OsmAnd::ObfReader(obfFile.data()));

    // Close current, reassign and load
    if(_obfFile && _obfFile->isOpen())
        _obfFile->close();
    unloadCurrentObf();
    _obfFile = obfFile;
    _obfReader = obfReader;
    loadCurrentObf();
}

void MainWindow::on_actionClose_triggered()
{
    if(_obfFile && _obfFile->isOpen())
        _obfFile->close();
    unloadCurrentObf();
}

void MainWindow::on_actionExit_triggered()
{
    if(_obfFile && _obfFile->isOpen())
        _obfFile->close();
    unloadCurrentObf();
    close();
}

void MainWindow::loadCurrentObf()
{
    setWindowTitle("OBF Viewer [" + _obfFile->fileName() + "]");

    loadPoiSearch();
}

void MainWindow::unloadCurrentObf()
{
    _obfFile.reset();
    _obfReader.reset();

    setWindowTitle("OBF Viewer");
    unloadPoiSearch();
}

void MainWindow::loadPoiSearch()
{

}

void MainWindow::unloadPoiSearch()
{
    ui->isPoiSearchLive->setChecked(false);
    ui->poiSearchCategories->clear();
    ui->poiSearchQuery->clear();
    ui->poiSearchResults->clear();
}

void MainWindow::on_poiSearchQuery_textEdited(const QString &arg1)
{

}

void MainWindow::on_poiSearchQuery_editingFinished()
{

}

void MainWindow::on_isPoiSearchLive_stateChanged(int arg1)
{

}
