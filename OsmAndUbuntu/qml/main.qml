import QtQuick 2.0
import Ubuntu.Components 0.1
import QtQuick.Window 2.0
MainView {
    //objectName for functional testing purposes (autopilot-qt5)
    objectName: "mainView"
    applicationName: "apcalc-qml"
    automaticOrientation:true;
    width: units.gu(70);
    height: units.gu(80);
    id:root
    Tabs {
        objectName: "Tabs"
        ItemStyle.class: "new-tabs"
        anchors.fill: parent
        id:mainWindow;
        Tab {
            objectName: "Calculator"
            title: "Calculator"
            Button {
                id: button
                text: "Press me"
                width: units.gu(15);
                anchors.centerIn: parent
                onClicked: {
                    text = applicationData.calculate(text);
                }
            }
        }
    }
}
