#!/bin/bash
# Проверка, что заданы входные и выходные файлы
if [[ $# -lt 2 ]]; then
    echo "Использование: $0 <входной файл> <выходной_файл> (osm или pbf)"
    echo "Пример: $0 input.osm output.osm"
    exit 1
fi

# Чтение аргументов
INPUT_OSM_FILE="$1"
OUTPUT_FILE="$2"
OVERPASS_URL="https://overpass.kumi.systems/api/interpreter"

# Проверка существования входного файла
if [[ ! -f "$INPUT_OSM_FILE" ]]; then
    echo "Ошибка: входной файл '$INPUT_OSM_FILE' не найден!"
    exit 1
fi

extension="${INPUT_OSM_FILE##*.}"
if [[ $extension == "osm" ]]; then
	# Попытка извлечь bbox из тега <bounds>
	bbox=$(grep -oP '<bounds\b[^>]*\b(minlat|minlon|maxlat|maxlon)=[^>]*>' "$INPUT_OSM_FILE" |
	    grep -oP 'minlat="[^"]*"|minlon="[^"]*"|maxlat="[^"]*"|maxlon="[^"]*"' |
	    awk -F'[="]' '{print $3}' | xargs | sed 's/ /,/g')

	# Если <bounds> не найден, вычисляем bbox по <node>
	if [[ -z "$bbox" ]]; then
	    echo "Внимание: в OSM XML файле отсутствует информация о bounds."
	    echo "Вычисляем bbox на основе координат узлов..."
    
	    # Извлечение всех значений lat и lon из <node>
	    latitudes=$(grep -oP '<node\b[^>]*\blat="[^"]*"' "$INPUT_OSM_FILE" | grep -oP 'lat="[^"]*"' | cut -d'"' -f2)
	    longitudes=$(grep -oP '<node\b[^>]*\blon="[^"]*"' "$INPUT_OSM_FILE" | grep -oP 'lon="[^"]*"' | cut -d'"' -f2)
    
	    # Проверка, что найдены координаты
	    if [[ -z "$latitudes" || -z "$longitudes" ]]; then
		echo "Ошибка: не удалось вычислить bbox. Файл не содержит узлов с координатами."
		exit 1
	    fi

	    # Вычисление минимальных и максимальных значений для lat и lon
	    minlat=$(echo "$latitudes" | sort -n | head -n1)
	    maxlat=$(echo "$latitudes" | sort -n | tail -n1)
	    minlon=$(echo "$longitudes" | sort -n | head -n1)
	    maxlon=$(echo "$longitudes" | sort -n | tail -n1)
    
	fi
elif [[ $extension == "pbf" ]]; then
	    minlat=$(osmconvert $INPUT_OSM_FILE --out-statistics | grep "lat min" | cut -c10-)
	    maxlat=$(osmconvert $INPUT_OSM_FILE --out-statistics | grep "lat max" | cut -c10-)
	    minlon=$(osmconvert $INPUT_OSM_FILE --out-statistics | grep "lon max" | cut -c10-)
	    maxlon=$(osmconvert $INPUT_OSM_FILE --out-statistics | grep "lon min" | cut -c10-)
fi

# Формирование bbox
bbox="$minlat,$maxlon,$maxlat,$minlon"

echo "BBox: $bbox"

# Формирование запроса к Overpass API
query="[out:xml][timeout:3600][maxsize:2000000000];
(
  node['place']($bbox);
);
out body;
>; 
out skel qt;"

echo "Отправка запроса на Overpass API..."

# Отправка запроса и сохранение результата
curl --silent --data-urlencode "data=$query" "$OVERPASS_URL" -o "$OUTPUT_FILE"

if [[ $? -eq 0 ]]; then
    echo "Результат успешно сохранен в $OUTPUT_FILE"
else
    echo "Ошибка! Не удалось получить данные с Overpass API."
    exit 1
fi

# Проверяем, установлена ли xmlstarlet
if ! command -v xmlstarlet &> /dev/null; then
    echo "Ошибка: xmlstarlet не установлен. Установите его, чтобы добавить атрибут version."
    exit 1
fi

echo "Проверка и добавление атрибута version=\"1\" для элементов node, way, relation..."

# Обновляем OSM XML файл, добавляя version="1", если его нет
xmlstarlet ed \
  -P -L \
  -i "//node[not(@version)]" -t attr -n version -v 1 \
  -i "//way[not(@version)]" -t attr -n version -v 1 \
  -i "//relation[not(@version)]" -t attr -n version -v 1 \
  "$OUTPUT_FILE"

if [[ $? -eq 0 ]]; then
    echo "Атрибут version=\"1\" добавлен для элементов node, way, relation (если отсутствовал)."
else
    echo "Ошибка при добавлении атрибута version."
    exit 1
fi