import csv
import argparse
import sys
from xml.sax.saxutils import escape  # Импорт функции для экранирования

# Уникальная функция для предотвращения двойного экранирования
def safe_escape(value):
    """
    Экранирует строку только тогда, когда она ещё не содержит экранированных символов.
    """
    # Проверяем на уже экранированные последовательности
    if isinstance(value, str):
        if '&amp;' in value or '<' in value or '>' in value or '&quot;' in value or '&apos;' in value:
            return value  # Строка уже экранирована
        return escape(value)  # Экранируем строку, если она не экранирована
    return str(value)

# Функция для создания строки узла в OSM XML
def create_node_str(node_id, lat, lon, street, city, postcode, housenumber=None, add_tags=False):
    parts = [f'<node id="{node_id}" lat="{lat}" lon="{lon}">']
    
    if add_tags:
        # Добавляем теги для улицы, города и почтового индекса
        for k, v in [('addr:street', street), ('addr:city', city), ('addr:postcode', postcode)]:
            parts.append(f'  <tag k="{safe_escape(k)}" v="{safe_escape(v)}" />')
        
        # Добавляем тег для номера дома, если указан
        if housenumber:
            parts.append(f'  <tag k="addr:housenumber" v="{safe_escape(str(housenumber))}" />')
    
    parts.append('</node>')
    return '\n'.join(parts)

# Генерация OSM XML из CSV с потоковой записью
def generate_osm_from_csv(input_csv, output_osm):
    # Подсчитаем количество строк в исходном файле
    with open(input_csv, newline='') as csvfile:
        total_rows = sum(1 for _ in csvfile) - 1  # Учитываем заголовок файла
    
    with open(input_csv, newline='') as csvfile, open(output_osm, 'w', encoding='utf-8') as outfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        
        # Пишем заголовок OSM файла
        outfile.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        outfile.write('<osm version="0.6" generator="python-csv-to-osm">\n')
        
        node_id = -10000  # Начальный id для узлов
        way_id = 1        # id для ways (линий)

        # Подсчёт обработанных строк
        processed_rows = 0
        last_reported_percent = 0

        # Обрабатываем каждую строку из CSV
        for row in reader:
            # Экранируем текстовые значения из CSV
            street = safe_escape(row['street'])
            city = safe_escape(row['city'])
            postcode = safe_escape(row['postcode'])
            interpolation = safe_escape(row['interpolation'])
            geometry = row['geometry']  # Геометрия не требует экранирования

            # Разбиваем геометрию (координаты)
            coords = geometry.replace('LINESTRING(', '').replace(')', '').split(',')
            nodes = []

            # Создаём узлы для первой, промежуточной и последней точки
            for idx, coord in enumerate(coords):
                lon, lat = map(float, coord.split())
                housenumber = None
                add_tags = False

                # Добавляем теги только для первой и последней точки
                if idx == 0:
                    housenumber = row['from']  # Начальная точка
                    add_tags = True
                elif idx == len(coords) - 1:
                    housenumber = row['to']  # Конечная точка
                    add_tags = True

                # Создаём узел и сразу записываем его в файл
                node_str = create_node_str(node_id, lat, lon, street, city, postcode, housenumber, add_tags)
                outfile.write(node_str + '\n')

                nodes.append(node_id)  # Запоминаем id узла для линии
                node_id -= 1  # Уменьшаем id для следующего узла

            # Создаём линию (way) и записываем её в файл
            outfile.write(f'<way id="{way_id}" version="1">\n')
            way_id += 1
            
            for node_ref in nodes:
                outfile.write(f'  <nd ref="{node_ref}" />\n')
            
            # Добавляем тег для интерполяции
            outfile.write(f'  <tag k="addr:interpolation" v="{interpolation}" />\n')
            outfile.write('</way>\n')

            # Обновляем информацию о выполнении
            processed_rows += 1
            current_percent = int((processed_rows / total_rows) * 100)

            # Печатаем прогресс только если он изменился
            if current_percent > last_reported_percent and current_percent % 10 == 0:  # Каждые 10%
                print(f"Прогресс: {current_percent}%")
                last_reported_percent = current_percent
                sys.stdout.flush()

        # Закрываем тег osm
        outfile.write('</osm>\n')

# Функция для обработки аргументов командной строки
def main():
    # Инициализируем парсер аргументов
    parser = argparse.ArgumentParser(description='Convert CSV with interpolated addresses to OSM XML format.')
    parser.add_argument('input', type=str, help='Input CSV file with interpolated addresses')
    parser.add_argument('output', type=str, help='Output OSM XML file')
    
    # Парсим аргументы
    args = parser.parse_args()

    # Генерируем OSM файл
    generate_osm_from_csv(args.input, args.output)

if __name__ == '__main__':
    main()