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

            page : Page {
                TextField {
                    id: text_input1

                    anchors.left: parent.left
                    anchors.right: button.left
                    anchors.leftMargin: units.gu(2);
                    anchors.rightMargin: units.gu(2);
                    anchors.top: parent.top
                    anchors.topMargin: units.gu(2);
                    text: "Enter osmand directory"
                    font.pixelSize: FontUtils.sizeToPixels("large")

                    height: units.gu(4)
                    Component.onCompleted: {
                        text = applicationData.getOsmandDirectiory();;
                    }

                    //font.pixelSize: units.gu(5);
                }
                Button {
                    id: button
                    text: "Select"
                    color: "green"
                    anchors.topMargin: units.gu(2);
                    anchors.leftMargin: units.gu(2);
                    anchors.rightMargin: units.gu(2);
                    anchors.top : parent.top
                    anchors.right: parent.right
                    width: units.gu(15);
                    onClicked: {
                        applicationData.setOsmandDirectiory(text_input1.text);
                        groupedModel.clear();
                        var files = applicationData.getFiles();
                        for(var i = 0; i < files.length; i++) {
                            var text = files[i].
                            replace('.obf', '').replace('_2','');
                            text = text.split('_').join(' ');
                            groupedModel.append({name:text});
                        }
                        text_input1.text = applicationData.getOsmandDirectiory();
                        PopupUtils.open(popoverComponent, button)
                    }
                }
                ListModel {
                    id: groupedModel
                }
                Component {
                    id: popoverComponent

                    Popover {
                        id: popover
                        Column {
                            id: containerLayout
                            anchors {
                                left: parent.left
                                top: parent.top
                                right: parent.right
                            }
                            ListItem.Header { text: "Standard list items" }
                            ListItem.Standard { text: "Do something" }
                            ListItem.Standard { text: "Do something else" }
                            ListItem.Header { text: "Buttons" }
                            ListItem.SingleControl {
                                highlightWhenPressed: false
                                control: Button {
                                    text: "Do nothing"
                                    anchors {
                                        fill: parent
                                        margins: units.gu(1)
                                    }
                                }
                            }
                            ListItem.SingleControl {
                                highlightWhenPressed: false
                                control: Button {
                                    text: "Close"
                                    anchors {
                                        fill: parent
                                        margins: units.gu(1)
                                    }
                                    onClicked: PopupUtils.close(popover)
                                }
                            }
                        }
                    }
                }

                Rectangle {
                    anchors.top : text_input1.bottom
                    anchors.topMargin: units.gu(2);
                    color: "#f7f7f7"
                    anchors.bottom: parent.bottom
                    width : parent.width
                    ListView {
                        id: groupedList
                        model: groupedModel
                        anchors.fill: parent
                        delegate: ListItem.Standard {
                            text: i18n.tr(name)
                            progression: true
                        }
                    }
                    Scrollbar {
                        flickableItem: groupedList
                        align: Qt.AlignTrailing
                    }
                }

            }
        }
    }

}
