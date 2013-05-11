import QtQuick 2.0
import Ubuntu.Components 0.1
import Ubuntu.Components.ListItems 0.1 as ListItem
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

            page : Page {
                TextField {
                    id: text_input1

                    anchors.left: parent.left
                    anchors.leftMargin: units.gu(2);
                    anchors.top: parent.top
                    anchors.topMargin: units.gu(2);
                    text: "Enter osmand directory"
                    font.pixelSize: FontUtils.sizeToPixels("large")
                    height: units.gu(4)
                    //font.pixelSize: units.gu(5);
                }
                Button {
                    id: button
                    text: "Select"
                    color: "green"
                    anchors.topMargin: units.gu(2);
                    anchors.leftMargin: units.gu(2);
                    anchors.top : parent.top
                    anchors.left: text_input1.right
                    width: units.gu(15);
                    onClicked: {
                        applicationData.setOsmandDirectiory(text_input1.text);
                    }
                }
                Column {
                    anchors.top : text_input1.bottom

                    ListItem.Caption {
                        text: "This is a caption text, which can span multiple lines."
                    }
                }
            }

        }
    }
}
