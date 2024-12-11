import csv
import re
import sys
import os
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import multiprocessing

def compile_replacement_pattern(abbreviations):
    """
    Создает одно регулярное выражение для замены всех аббревиатур на полные имена.
    """
    pattern = r'\b(' + '|'.join(re.escape(abbr) for abbr in abbreviations.keys()) + r')\b'
    replacements = {abbr: full_name.capitalize() for abbr, full_name in abbreviations.items()}
    return pattern, replacements

def replace_abbreviations_in_line(line, pattern, replacements):
    """
    Заменяет аббревиатуры на полные имена в строке с использованием одного регулярного выражения.
    """
    def replacement_func(match):
        abbr = match.group(0)
        return replacements[abbr.lower()]
    
    return re.sub(pattern, replacement_func, line, flags=re.IGNORECASE)

def process_line_osm(line, pattern, replacements):
    """
    Обрабатывает строку OSM: заменяет аббревиатуры на полные имена, если строка содержит тег addr:street.
    """
    if 'addr:street' in line:
        return replace_abbreviations_in_line(line, pattern, replacements)
    return line

def process_csv_row(row, street_column, pattern, replacements):
    """
    Обрабатывает строку CSV: заменяет аббревиатуры в колонке street.
    """
    if street_column in row:
        row[street_column] = replace_abbreviations_in_line(row[street_column], pattern, replacements)
    return row

def process_osm_file(input_file, output_file, pattern, replacements, buffer_size=50 * 1024 * 1024):
    """
    Обрабатывает OSM XML файл с заменой аббревиатур.
    """
    # Открытие входного и выходного файлов
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
        total_lines = sum(1 for _ in infile)
        infile.seek(0)  # Сбросить указатель на начало файла

        pbar = tqdm(total=total_lines, desc="Обработка строк OSM", unit="строка")

        current_size = 0
        buffer = []

        # Чтение и обработка строк
        for line in infile:
            processed_line = process_line_osm(line, pattern, replacements)
            buffer.append(processed_line)
            current_size += len(processed_line.encode('utf-8'))

            if current_size >= buffer_size:
                outfile.writelines(buffer)
                buffer.clear()
                current_size = 0

            pbar.update(1)

        if buffer:
            outfile.writelines(buffer)

        pbar.close()

def process_csv_file(input_file, output_file, pattern, replacements, buffer_size=50 * 1024 * 1024):
    """
    Обрабатывает CSV файл с заменой аббревиатур в колонке street, сбрасывая данные на диск каждые 50 МБ.
    """
    # Сначала подсчитаем общее количество строк в входном CSV файле
    total_lines = sum(1 for _ in open(input_file, 'r', encoding='utf-8')) - 1  # -1 чтобы исключить строку с заголовком

    with open(input_file, 'r', newline='', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile, delimiter=';')
        fieldnames = reader.fieldnames

        if "street" not in fieldnames:
            raise ValueError("Входной CSV файл должен содержать колонку 'street'.")

        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()

        # Обновляем tqdm, добавляя общее количество строк
        pbar = tqdm(total=total_lines, desc="Обработка строк CSV", unit="строка", unit_scale=True)

        buffer = []  # Буфер для строк
        current_size = 0  # Текущий размер буфера в байтах

        for row in reader:
            # Обработка текущей строки
            processed_row = process_csv_row(row, "street", pattern, replacements)
            
            # Преобразование строки в CSV-формат и добавление в буфер
            csv_line = ";".join([processed_row[field] for field in fieldnames]) + "\n"
            buffer.append(csv_line)
            current_size += len(csv_line.encode('utf-8'))

            # Если размер буфера достигает 50 МБ, сбрасываем на диск
            if current_size >= buffer_size:
                outfile.writelines(buffer)
                buffer.clear()  # Очищаем буфер
                current_size = 0  # Обнуляем размер

            pbar.update(1)

        # Записываем оставшиеся данные
        if buffer:
            outfile.writelines(buffer)

        pbar.close()

def process_file(input_file, csv_file, output_file, buffer_size=50 * 1024 * 1024):
    """
    Определяет тип файла входных данных (OSM или CSV) и обрабатывает его.
    """
    # Чтение CSV файла с аббревиатурами
    abbreviations = {}
    with open(csv_file, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            full_name, abbr = row
            abbreviations[abbr.strip()] = full_name.strip()

    # Создание регулярного выражения и словаря замен
    pattern, replacements = compile_replacement_pattern(abbreviations)

    # Проверка типа входного файла
    if input_file.endswith('.osm'):
        process_osm_file(input_file, output_file, pattern, replacements, buffer_size)
    elif input_file.endswith('.csv'):
        process_csv_file(input_file, output_file, pattern, replacements)
    else:
        raise ValueError("Неверный формат входного файла. Поддерживаются только '.osm' и '.csv'.")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python replace_abbreviations.py <input_file> <abbreviations.csv> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    csv_file = sys.argv[3]

    process_file(input_file, csv_file, output_file)