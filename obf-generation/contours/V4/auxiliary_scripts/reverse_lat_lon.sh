#!/bin/bash
sed -i s/lat/l1t/g $1
sed -i s/lon/lat/g $1
sed -i s/l1t/lon/g $1