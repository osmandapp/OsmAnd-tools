import QtQuick 2.0
import Ubuntu.Components 0.1
import Ubuntu.Components.ListItems 0.1 as ListItem
import Ubuntu.Components.Popups 0.1
import QtQuick.Window 2.0
MainView {
    //objectName for functional testing purposes (autopilot-qt5)
    objectName: "mainView"
    applicationName: "apcalc-qml"
    automaticOrientation:true;
    width: units.gu(60);
    height: units.gu(90);
    id:root
    Tabs {
        objectName: "Tabs"
        ItemStyle.class: "new-tabs"
        anchors.fill: parent
        id:mainWindow;
        Tab {
            title: "Settings"
            id:tab;
            x: 0
            y: 0
            anchors.rightMargin: 0
            anchors.bottomMargin: 0
            anchors.leftMargin: 0
            anchors.topMargin: 0
            page : Loader {
                anchors {
                    fill : parent
                }
                source : "files.qml"
            }
        }
    }
}
