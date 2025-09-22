#!/bin/bash -eu -o pipefail

wget --quiet --output-document=mangrove.json 'https://api.mangrove.reviews/reviews?latest_edits_only=false'
