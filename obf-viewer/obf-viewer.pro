QT += core gui widgets

TARGET = obf-viewer
TEMPLATE = app

INCLUDEPATH += \
    "$$_PRO_FILE_PWD_/../../core/include" \
    "$$_PRO_FILE_PWD_/../../core/protos" \
    "$$_PRO_FILE_PWD_/../../core/externals/protobuf/upstream.patched/src"

win32:TARGET_OS = windows
unix:TARGET_OS = linux
equals(QMAKE_TARGET.arch, x86) {
    ARCH = i686
}
equals(QMAKE_TARGET.arch, x86_64) {
    ARCH = amd64
}
CONFIG(release):FLAVOR = Release
CONFIG(debug):FLAVOR = Debug
DEPENDPATH += "$$_PRO_FILE_PWD_/../../binaries/$$TARGET_OS/$$ARCH/$$FLAVOR"

win32:LIBS += OsmAndCore.lib
unix:LIBS += -lOsmAndCore

SOURCES += \
	main.cpp \
	mainwindow.cpp

HEADERS += \
	mainwindow.h

FORMS += \
	mainwindow.ui
