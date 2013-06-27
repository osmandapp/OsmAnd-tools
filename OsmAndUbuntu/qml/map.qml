import QtQuick 2.0
import Ubuntu.Components 0.1
import Ubuntu.Components.ListItems 0.1 as ListItem
import Ubuntu.Components.Popups 0.1
import QtQuick.Window 2.0


Page {
    Canvas {
        id: canvas;

        property variant paths : [[]];

        property int mapMargin : 5;
        property int counter : 0;

        property variant imgToDraw;
        property variant nimgToDraw;
        property double tileX;
        property double tileY;
        property double ntileX;
        property double ntileY;

        anchors.top : parent.top
        anchors.bottom: parent.bottom
        width : parent.width
        antialiasing: true;
        onPaint: {
            var context = canvas.getContext("2d");
            context.clearRect(0, 0, canvas.width, canvas.height);
            mapViewLayer.left(imgToDraw)

            var l  = mapViewLayer.left(imgToDraw);
            var t  = mapViewLayer.top(imgToDraw);
            context.drawImage(imgToDraw, l - mapMargin, t - mapMargin, canvas.width + 2 * mapMargin,
                              canvas.height + 2 * mapMargin);

            context.save();
            drawRouteLayer(context);
            context.restore();

            context.save();
            drawTargetLocation(context);
            context.restore();

            context.save();
            drawStartLocation(context);
            context.restore();
        }

        onImageLoaded:  {
            canvas.unloadImage(imgToDraw, 0, 0);
            imgToDraw = nimgToDraw;
            canvas.requestPaint();
        }
        onCanvasSizeChanged: {
            mapViewAdapter.setBounds(canvas.width + 2* mapMargin, canvas.height + 2* mapMargin);
            refreshMap(true);
        }

        Component.onCompleted: {
            var z = mapLayerData.getMapZoom();
            mapViewAdapter.setZoom(z);
            mapViewAdapter.setLatLon(mapLayerData.getMapLatitude(), mapLayerData.getMapLongitude());
            mapLayerData.mapNeedsToRefresh.connect(refreshMapMessage);
            requestPaint();
        }

        Component.onDestruction: {
            mapLayerData.setMapLatLonZoom(mapViewAdapter.getLat(), mapViewAdapter.getLon(), mapViewAdapter.getZoom());
        }

        ActivityIndicator {
            id: activity;
            anchors.right: parent.right
            anchors.top: parent.top
            anchors.margins: units.gu(1)
        }

        Button {
            id : rotRight
            text : 'R'
            anchors.top: parent.top
            anchors.right: activity.left
            anchors.margins: units.gu(1)
            onClicked: {
                var r = mapViewAdapter.getRotate() ;
                mapViewAdapter.setRotate(r+ 30);
                refreshMap(true);
            }
        }
        Button {
            id : rotLeft
            text : 'L'
            anchors.right: rotRight.left
            anchors.top: parent.top
            anchors.margins: units.gu(1)
            onClicked: {
                mapViewAdapter.setRotate(mapViewAdapter.getRotate() - 30);
                refreshMap(true);
            }
        }

        Button {
            id : zoomIn
            color: "green"
            text : '+'
            anchors.bottom: parent.bottom
            anchors.right: parent.right
            anchors.margins: units.gu(1)
            onClicked: {
                mapViewAdapter.setZoom(mapViewAdapter.getZoom() + 1);
                refreshMap(true);
            }
        }
        Button {
            id : zoomOut
            color: "green"
            text : '-'
            anchors.right: zoomIn.left
            anchors.bottom: parent.bottom
            anchors.margins: units.gu(1)
            onClicked: {
                mapViewAdapter.setZoom(mapViewAdapter.getZoom() - 1);
                refreshMap(true);
            }
        }

        MouseArea {
            id : marea;
            property bool pressed;
            property int px;
            property int py;
            anchors.fill: parent
            acceptedButtons : 'AllButtons'
            onPressed : {
                if(mouse.button === 2) {
                    var o = PopupUtils.open(popoverComponent, canvas);
                    o.lat = mapViewAdapter.getRotatedMapLatForPoint(mouse.x, mouse.y);
                    o.lon = mapViewAdapter.getRotatedMapLonForPoint(mouse.x, mouse.y);
                } else if(rcontains(zoomIn, mouse.x, mouse.y)) {
                    mouse.accepted =false;
                } else if(rcontains(zoomOut, mouse.x, mouse.y)) {
                    mouse.accepted =false;
                } else if(rcontains(rotLeft, mouse.x, mouse.y)) {
                    mouse.accepted =false;
                } else if(rcontains(rotRight, mouse.x, mouse.y)) {
                    mouse.accepted =false;
                } else {
                    mouse.accepted = true;
                    px = mouse.x;
                    py = mouse.y;
                    pressed = true;
                }
            }

            onPositionChanged : {
                if(marea.pressed) {
                    mapViewAdapter.moveTo( px - mouse.x, py - mouse.y);
                    px = mouse.x;
                    py = mouse.y;
                    refreshMap();
                }
            }

            onReleased: {
                if(marea.pressed) {
                    marea.pressed = false;
                    mapViewAdapter.moveTo( px - mouse.x, py - mouse.y);
                    refreshMap();
                }
            }
        }

    }

    Component {
        id: popoverComponent
        Popover {
            id: popover
            property variant lat;
            property variant lon;
            Column {
                id: containerLayout
                anchors {
                    left: parent.left
                    top: parent.top
                    right: parent.right
                }
                ListItem.Header {
                    text: "Local actions";
                }
                ListItem.SingleControl {
                    highlightWhenPressed: false
                    control: Button {
                        text: "Set destination point"
                        anchors {
                            fill: parent
                            margins: units.gu(1)
                        }
                        onClicked: {
                            mapLayerData.setTargetLatLon(lat, lon);
                            PopupUtils.close(popover)
                            refreshMap(true);
                        }
                    }
                }
                ListItem.SingleControl {
                    highlightWhenPressed: false
                    control: Button {
                        text: "Set start point"
                        anchors {
                            fill: parent
                            margins: units.gu(1)
                        }
                        onClicked: {
                            mapLayerData.setStartLatLon(lat, lon);
                            PopupUtils.close(popover)
                            refreshMap(true);
                        }
                    }
                }
                ListItem.SingleControl {
                    highlightWhenPressed: false
                    control: Button {
                        text: "Calculate route"
                        anchors {
                            fill: parent
                            margins: units.gu(1)
                        }
                        onClicked: {
                            PopupUtils.close(popover)
                            mapActions.calculateRoute();
                            refreshMap(true);
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

    function rcontains(item, x, y) {
        var v = mapToItem(item, x, y);
        if(v.x>= 0 && v.x<= item.width && v.y >= 0 && v.y <=item.height) {
            return true;
        }
        return false;
    }

    function refreshMapMessage(msg) {
        if(msg) console.log(msg);
        refreshMap(true);
    }

    function refreshMap(force) {

        activity.running = mapActions.isActivityRunning();
        if(canvas.nimgToDraw === canvas.imgToDraw || force) {
            canvas.counter ++;
            canvas.ntileX = mapViewAdapter.getXTile();
            canvas.ntileY = mapViewAdapter.getYTile();
            canvas.nimgToDraw = "image://map/map"+canvas.counter;
            canvas.loadImage(canvas.nimgToDraw, 0, 0);
        }
        canvas.requestPaint();
    }


    function drawRouteLayer(context) {
        if(mapLayerData.getRoutePointLength() >  0) {
            context.strokeStyle = Qt.rgba(0, 0,0.9,0.8);
            context.lineWidth = 6;
            context.beginPath();
            for(var i = 0; i < mapLayerData.getRoutePointLength(); i++) {
                var lat = mapLayerData.getRoutePointLat(i);
                var lon = mapLayerData.getRoutePointLon(i);
                var x = mapViewAdapter.getRotatedMapXForPoint(lat, lon);
                var y = mapViewAdapter.getRotatedMapYForPoint(lat, lon);
                if(i == 0) {
                    context.moveTo(x, y);
                } else {
                    context.lineTo(x, y);
                }
            }
            context.stroke();
            context.beginPath();
            context.lineWidth = 1;
            context.strokeStyle = Qt.rgba(0, 0,0,1);
            if(mapViewAdapter.getZoom() > 15) {
                for(var i = 0; i < mapLayerData.getRoutePointLength(); i++) {
                    var txt = mapLayerData.getRoutePointText(i);
                    if(txt != "") {
                        var lat = mapLayerData.getRoutePointLat(i);
                        var lon = mapLayerData.getRoutePointLon(i);
                        var x = mapViewAdapter.getRotatedMapXForPoint(lat, lon);
                        var y = mapViewAdapter.getRotatedMapYForPoint(lat, lon);
                        context.strokeText(txt, x, y);
                        context.fillText(txt, x, y);
                    }
                }
            }
        }
    }

    function drawTargetLocation(context) {
        if(mapLayerData.isTargetPresent()) {
            var x = mapViewAdapter.getRotatedMapXForPoint(mapLayerData.getTargetLatitude(), mapLayerData.getTargetLongitude());
            var y = mapViewAdapter.getRotatedMapYForPoint(mapLayerData.getTargetLatitude(), mapLayerData.getTargetLongitude());
            context.fillStyle = 'rgba(200, 10, 10, 0.8)';
            context.strokeStyle = 'rgb(0, 0, 0)'
            context.beginPath();

            context.arc(x, y, 5, 0, 360, true);
            context.closePath();
            context.fill();
            context.stroke();
        }
    }

    function drawStartLocation(context) {
        if(mapLayerData.isStartPresent()) {
            var x = mapViewAdapter.getRotatedMapXForPoint(mapLayerData.getStartLatitude(), mapLayerData.getStartLongitude());
            var y = mapViewAdapter.getRotatedMapYForPoint(mapLayerData.getStartLatitude(), mapLayerData.getStartLongitude());
            context.fillStyle = 'rgba(10, 200, 10, 0.8)';
            context.strokeStyle = 'rgb(0, 0, 0)'
            context.beginPath();
            context.arc(x, y, 5, 0, 360, true);
            context.closePath();
            context.fill();
            context.stroke();
        }
    }

}
