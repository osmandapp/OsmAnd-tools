import QtQuick 2.0
import Ubuntu.Components 0.1
import Ubuntu.Components.ListItems 0.1 as ListItem
import Ubuntu.Components.Popups 0.1
import QtQuick.Window 2.0


Page {


    Canvas {
        property bool loaded;
        property variant paths : [[]];
        id: canvas;
        anchors.top : parent.top
        anchors.bottom: parent.bottom
        width : parent.width
        antialiasing: true;
        onPaint: {
            var ctx = canvas.getContext("2d");
            if(loaded) {
                var context = canvas.getContext("2d");
                context.clearRect(0, 0, canvas.width, canvas.height);
                var left = Math.floor(mapData.getTiles().x);
                var top = Math.floor(mapData.getTiles().y);
                var tileX = mapData.getXTile();
                var tileY = mapData.getYTile();
                var w = mapData.getCenterPointX();
                var h = mapData.getCenterPointY();
                var ftileSize = mapData.getTileSize();
                for (var i = 0; i < paths.length; i++) {
                    for (var j = 0; j < paths[i].length; j++) {
                        var leftPlusI = left + i;
                        var topPlusJ = top + j;
                        var x1 = (left + i - tileX) * ftileSize + w;
                        var y1 = (top + j - tileY) * ftileSize + h;
                        context.drawImage(paths[i][j],  x1, y1, ftileSize, ftileSize);
                    }
                }
            }
        }

        onImageLoaded:  {
            loaded = true;
            canvas.requestPaint();
        }
        onCanvasSizeChanged: {
            mapData.setBounds(canvas.width, canvas.height);
            mapData.setZoom(5);
            mapData.setLatLon(52, 4);
            refreshMap();
        }

        Button {
            id : zoomIn
            color: "green"
            text : '+'
            anchors.bottom: parent.bottom
            anchors.right: parent.right
            anchors.margins: units.gu(1)
            onClicked: {
                mapData.setZoom(mapData.getZoom() + 1);
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
                mapData.setZoom(mapData.getZoom() - 1);
                refreshMap();
            }
        }

        MouseArea {
            id : marea;
            property bool pressed;
            property int px;
            property int py;
            anchors.fill: parent
            onPressed : {
                if(rcontains(zoomIn, mouse.x, mouse.y)) {
                    mouse.accepted =false;
                } else if(rcontains(zoomOut, mouse.x, mouse.y)) {
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
                    mapData.moveTo( px - mouse.x, py - mouse.y);
                    px = mouse.x;
                    py = mouse.y;
                    refreshMap();
                }
            }

            onReleased: {
                if(marea.pressed) {
                    marea.pressed = false;
                    mapData.moveTo( px - mouse.x, py - mouse.y);
                    refreshMap();
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

    function refreshMap() {
        var left = Math.floor(mapData.getTiles().x);
        var top = Math.floor(mapData.getTiles().y);
        var width  = Math.ceil(mapData.getTiles().x + mapData.getTiles().width) - left;
        var height  = Math.ceil(mapData.getTiles().y + mapData.getTiles().height) - top;
        var base = applicationData.getOsmandDirectiory() + "/tiles/Mapnik/";
        base = base + mapData.getZoom()+"/";
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

}
