import QtQuick 2.0
import Ubuntu.Components 0.1
import Ubuntu.Components.ListItems 0.1 as ListItem
import Ubuntu.Components.Popups 0.1
import QtQuick.Window 2.0


Page {
    Canvas {
        id: canvas;
        property bool loaded;
        property variant paths : [[]];
        anchors.top : parent.top
        anchors.bottom: parent.bottom
        width : parent.width
        antialiasing: true;
        onPaint: {
            var ctx = canvas.getContext("2d");
            if(true) {
                var context = canvas.getContext("2d");
                context.clearRect(0, 0, canvas.width, canvas.height);
                context.save();
                drawLayerMap(context);
                context.restore();

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
        }

        onImageLoaded:  {
            loaded = true;
            canvas.requestPaint();
        }
        onCanvasSizeChanged: {
            mapViewAdapter.setBounds(canvas.width, canvas.height);
            refreshMap();
        }

        Component.onCompleted: {
            var z = mapLayerData.getMapZoom();
            mapViewAdapter.setZoom(z);
            mapViewAdapter.setLatLon(mapLayerData.getMapLatitude(), mapLayerData.getMapLongitude());
            mapLayerData.mapNeedsToRefresh.connect(refreshMapMessage);
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
                refreshMap();
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
                refreshMap();
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
                refreshMap();
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
                refreshMap();
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
                            refreshMap();
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
                            refreshMap();
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
                            refreshMap();
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
        refreshMap();
    }

    function refreshMap() {
        activity.running = mapActions.isActivityRunning();
        var left = Math.floor(mapViewAdapter.getTiles().x);
        var top = Math.floor(mapViewAdapter.getTiles().y);
        var width  = Math.ceil(mapViewAdapter.getTiles().x + mapViewAdapter.getTiles().width) - left;
        var height  = Math.ceil(mapViewAdapter.getTiles().y + mapViewAdapter.getTiles().height) - top;
        var base = applicationData.getOsmandDirectiory() + "/tiles/Mapnik/";
        base = base + mapViewAdapter.getZoom()+"/";
        var p = new Array(width);
        for (var i = 0; i < width; i++) {
            p[i]= new Array(height);
            for (var j = 0; j < height; j++) {
                var s = base +  (left + i)+"/"+(top+j)+".png.tile";
                p[i][j] = s;
                canvas.loadImage(p[i][j]);
            }
        }
        canvas.paths = p;
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
            for(var i = 0; i < mapLayerData.getRoutePointLength(); i++) {
                var lat = mapLayerData.getRoutePointLat(i);
                var lon = mapLayerData.getRoutePointLon(i);
                var x = mapViewAdapter.getRotatedMapXForPoint(lat, lon);
                var y = mapViewAdapter.getRotatedMapYForPoint(lat, lon);
                context.strokeText((i+1)+".", x, y -i);
                context.fillText((i+1)+".", x, y -i);
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

    function drawLayerMap(context) {
        context.translate(mapViewAdapter.getCenterPointX(), mapViewAdapter.getCenterPointY());
        context.rotate((mapViewAdapter.getRotate() / 180) *Math.PI  );
        context.translate(-mapViewAdapter.getCenterPointX(), -mapViewAdapter.getCenterPointY());
        //context.drawImage("image://map/map"+mapViewAdapter.getRotate(), 0, 0);
        var left = Math.floor(mapViewAdapter.getTiles().x);
        var top = Math.floor(mapViewAdapter.getTiles().y);
        var tileX = mapViewAdapter.getXTile();
        var tileY = mapViewAdapter.getYTile();
        var w = mapViewAdapter.getCenterPointX();
        var h = mapViewAdapter.getCenterPointY();
        var ftileSize = mapViewAdapter.getTileSize();
        for (var i = 0; i < canvas.paths.length; i++) {
            for (var j = 0; j < canvas.paths[i].length; j++) {
                var leftPlusI = left + i;
                var topPlusJ = top + j;
                var x1 = (left + i - tileX) * ftileSize + w;
                var y1 = (top + j - tileY) * ftileSize + h;
                context.drawImage(canvas.paths[i][j],  x1, y1, ftileSize, ftileSize);
            }
        }
    }

}
