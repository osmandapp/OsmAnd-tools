#!/usr/bin/env python3

import json
import logging
import sys
import typing
import urllib.request

JSON: typing.TypeAlias = (
    dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None
)

PAGE_SIZE = 1000
PAGE_URL = "https://api.mangrove.reviews/reviews?latest_edits_only=false&limit={limit}&offset={offset}"


def read_page(offset: int) -> list[JSON]:
    logging.debug("reading with offset %d", offset)
    with urllib.request.urlopen(PAGE_URL.format(limit=1000, offset=offset)) as f:
        reviews = json.load(f)["reviews"]
        logging.info("read %d reviews", len(reviews))
    return reviews


def read_reviews(output_file: str) -> None:
    reviews = []
    while True:
        reviews_page = read_page(len(reviews))
        if len(reviews_page) == 0:
            break
        reviews.extend(reviews_page)
    with open(output_file, "w") as f:
        json.dump(dict(reviews=reviews), f, indent=1)
    logging.info("wrote %d reviews to %s", len(reviews), output_file)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} <output_file>")
        exit(2)
    output_file = sys.argv[1]
    read_reviews(output_file)
