#!/usr/bin/env python

#  Installation:
#  pip install lxml
#  pip install beautifulsoup4
#
#  Run:
#  python generate_svg_map_legend.py


from bs4 import BeautifulSoup
import json
import os

CANVAS_WIDTH: int = 300
CANVAS_HEIGHT: int = 35
CIRCLE_SIZE_OUTER: int = 19
CIRCLE_SIZE_INNER: int = 18
ICON_SIZE: int = 12
ICON_CONTENT_PLACEHOLDER: str = 'ICON_CONTENT'
ICONS_FOLDER: str = '../../resources/icons/svg/'
RESULT_FOLDER: str = 'Result'


def read_config() -> dict[str, str]:
    try:
        content: str = open('config.json', 'r').read()
        json_dict: dict[str, str] = json.loads(content)
        return json_dict
    except Exception as e:
        print(f'Error: Failed reading config.json file \n{e}\n')
        return {}


def build_svg_for_config_items(json_dict: dict[str, str]):
    for icon_config in json_dict:
        try:
            icon: str = get_icon_svg_content(icon_config['icon_name'], icon_config['icon_color'])

            background: str = get_background_objects_svg_content(icon_config['background_color'],
                                                                 float(icon_config['background_opacity']),
                                                                 icon_config['icon_shield_color'],
                                                                 icon_config['icon_shield_stroke_color'],
                                                                 icon_config['icon_shield_hidden'] == 'True')

            composed_svg_content: str = background.replace(ICON_CONTENT_PLACEHOLDER, icon)
            build_svg(icon_config['icon_name'], composed_svg_content)
        except Exception as e:
            print(f'Error: Failed to build icon for config string: \n{icon_config} \n{e}\n')


def build_svg(resouces_path: str, content: str):
    path_components: [str] = resouces_path.split('/')
    filename: str = path_components[len(path_components) - 1]
    if not os.path.exists(RESULT_FOLDER):
        os.mkdir(RESULT_FOLDER)
    with open(f'{RESULT_FOLDER}/{filename}', 'w') as file:
        file.write(content)


def get_background_objects_svg_content(background_color: str, background_opacity: float,
                                       icon_shield_color: str, icon_shield_stroke_color: str, icon_shield_hidden: bool, ) -> str:
    result: str = '<svg width="300" height="35" viewBox="0 0 300 35" fill="none" xmlns="http://www.w3.org/2000/svg">\n\n'

    # background colored rectangle
    result += f'<rect width="300" height="35" fill="{background_color}" fill-opacity="{background_opacity}"/>\n'

    # background colored icon circle
    if not icon_shield_hidden:
        result += f'<path opacity="0.953" fill="{icon_shield_color}" d="M149.999 9C154.963 9 159 13.0018 159 18C159 22.9655 154.963 27 149.999 27C145.037 27 141 22.9655 141 18C141 13.0018 145.037 9 149.999 9Z"/>\n'
        result += f'<path opacity="0.95" fill="{icon_shield_stroke_color}" fill-rule="evenodd" clip-rule="evenodd" d="M150 27C154.971 27 159 22.9706 159 18C159 13.0294 154.971 9 150 9C145.029 9 141 13.0294 141 18C141 22.9706 145.029 27 150 27ZM150 28C155.523 28 160 23.5228 160 18C160 12.4772 155.523 8 150 8C144.477 8 140 12.4772 140 18C140 23.5228 144.477 28 150 28Z"/>\n\n'

    result += ICON_CONTENT_PLACEHOLDER + '\n'
    result += '</svg>'
    return result


def get_icon_svg_content(icon_path: str, icon_color: str) -> str:
    original_icon_content: str = open(f'{ICONS_FOLDER}/{icon_path}', 'r').read()
    parser: BeautifulSoup = BeautifulSoup(original_icon_content, 'xml')

    if len(parser.findAll('svg')) > 0:
        svg_tag = parser.findAll('svg')[0]

        # change color for all icon lines
        for subtag in svg_tag.findAll():
            if 'fill' in subtag.attrs:
                subtag.attrs['fill'] = icon_color

        # convert all inner svg content to plain text
        icon_content: str = ''
        for subtag in svg_tag.contents:
            icon_content += str(subtag);

        # put processed icon content inside new tag to scale it and move ti to center
        original_icon_width: float = 580
        if 'width' in svg_tag.attrs:
            original_icon_width = int(svg_tag.attrs['width'])

        icon_scaling_ratio: float = ICON_SIZE / int(original_icon_width)
        icon_offset_x: float = CANVAS_WIDTH / 2 - ICON_SIZE / 2
        icon_offset_y: float = CANVAS_HEIGHT / 2 - ICON_SIZE / 2
        return f'<g transform="translate({icon_offset_x}, {icon_offset_y}) scale({icon_scaling_ratio} {icon_scaling_ratio}) "> \n{icon_content} \n</g>'

    return ''


# Script starts here
if __name__ == '__main__':
    config: dict[str, str] = read_config()
    build_svg_for_config_items(config)
