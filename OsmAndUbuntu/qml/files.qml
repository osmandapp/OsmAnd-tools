import QtQuick 2.0
import Ubuntu.Components 0.1
import Ubuntu.Components.ListItems 0.1 as ListItem
import Ubuntu.Components.Popups 0.1
import QtQuick.Window 2.0


Page {
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
            reloadList();
        }
    }
    ListModel {
        id: groupedModel
    }
    Rectangle {
        anchors.top : text_input1.bottom
        anchors.topMargin: units.gu(2);
        color: "#f7f7f7"
        clip: true
        anchors.bottom: parent.bottom
        width : parent.width
        ListView {
            id: groupedList
            model: groupedModel
            anchors.fill: parent
            delegate: ListItem.Standard {
                text: i18n.tr(name)
                progression: true
                onClicked : {
                    groupedList.currentIndex = model.index;
                    PopupUtils.open(popoverComponent, groupedList);
                }
            }
            Component.onCompleted: {
                reloadList();
            }
        }
        Scrollbar {
            flickableItem: groupedList
            align: Qt.AlignTrailing
        }
    }
    function reloadList() {
        groupedModel.clear();
        var files = applicationData.getFiles();
        for(var i = 0; i < files.length; i++) {
            var text = files[i].
            replace('.obf', '').replace('_2','');
            text = text.split('_').join(' ');
            groupedModel.append({name:text});
        }
        if(applicationData.getOsmandDirectiory() !== "") {
            text_input1.text = applicationData.getOsmandDirectiory();
        }
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
                ListItem.Header {
                    text: groupedModel.get(groupedList.currentIndex).name;
                }
                ListItem.SingleControl {
                    highlightWhenPressed: false
                    height:  units.gu(30);
                    Flickable {
                        id : textItem
                        anchors.fill: parent
                        contentHeight: textItemSingle.height
                        clip: true
                        Text {
                            id : textItemSingle
                            wrapMode:  Text.WrapAtWordBoundaryOrAnywhere
                            width : parent.width- units.gu(2)
                            x:  parent.x + units.gu(1)
                            text: applicationData.describeFile(groupedList.currentIndex);
                        }
                    }
                    Scrollbar {
                        flickableItem: textItem
                        align: Qt.AlignTrailing
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
}
