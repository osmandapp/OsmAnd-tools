#!/usr/bin/python3
# Run QGIS algorhytms

# Append the path where processing plugins can be found
import sys
sys.path.append('/usr/share/qgis/python/plugins/')
import argparse

from qgis.core import *
from qgis.analysis import QgsNativeAlgorithms

from qgis.core import *
import processing
from processing.core.Processing import Processing
from processing.tools import dataobjects

# See https://gis.stackexchange.com/a/155852/4972 for details about the prefix
# Initialize QGIS Application
qgs = QgsApplication([], False)
QgsApplication.setPrefixPath("/usr", True)
QgsApplication.initQgis()

Processing.initialize()
QgsApplication.processingRegistry().addProvider(QgsNativeAlgorithms())

parser = argparse.ArgumentParser()
parser.add_argument("-alg")
parser.add_argument("-param1")
parser.add_argument("-param2")
parser.add_argument("-param3")
parser.add_argument("-param4")
parser.add_argument("-param5")
parser.add_argument("-param6")
parser.add_argument("-param7")
parser.add_argument("-param8")
parser.add_argument("-param9")
parser.add_argument("-param10")
parser.add_argument("-value1")
parser.add_argument("-value2")
parser.add_argument("-value3")
parser.add_argument("-value4")
parser.add_argument("-value5")
parser.add_argument("-value6")
parser.add_argument("-value7")
parser.add_argument("-value8")
parser.add_argument("-value9")
parser.add_argument("-value10")
args = parser.parse_args()
if args.alg:
     print("\033[95malg: "+args.alg+"\033[0m")
if args.param1 and args.value1:
     print(args.param1+": "+args.value1)
if args.param2 and args.value2:
     print(args.param2+": "+args.value2)
if args.param3 and args.value3:
     print(args.param3+": "+args.value3)
if args.param4 and args.value4:
     print(args.param4+": "+args.value4)
if args.param5 and args.value5:
     print(args.param5+": "+args.value5)
if args.param6 and args.value6:
     print(args.param6+": "+args.value6)
if args.param7 and args.value7:
     print(args.param7+": "+args.value7)
if args.param8 and args.value8:
     print(args.param8+": "+args.value8)
if args.param9 and args.value9:
     print(args.param9+": "+args.value9)
if args.param10 and args.value10:
     print(args.param10+": "+args.value10)

if args.value1 and args.value1.startswith("[") :
     args.value1=args.value1[1:-1].split(',')
if args.value2 and args.value2.startswith("[") :
     args.value2=args.value2[1:-1].split(',')
if args.value3 and args.value3.startswith("[") :
     args.value3=args.value3[1:-1].split(',')
if args.value4 and args.value4.startswith("[") :
     args.value4=args.value4[1:-1].split(',')
if args.value5 and args.value5.startswith("[") :
     args.value5=args.value5[1:-1].split(',')
if args.value6 and args.value6.startswith("[") :
     args.value6=args.value6[1:-1].split(',')
if args.value7 and args.value7.startswith("[") :
     args.value7=args.value7[1:-1].split(',')
if args.value8 and args.value8.startswith("[") :
     args.value8=args.value8[1:-1].split(',')
if args.value9 and args.value9.startswith("[") :
     args.value9=args.value9[1:-1].split(',')
if args.value10 and args.value10.startswith("[") :
     args.value10=args.value10[1:-1].split(',')

params = {
     args.param1:args.value1,
     args.param2:args.value2,
     args.param3:args.value3,
     args.param4:args.value4,
     args.param5:args.value5,
     args.param6:args.value6,
     args.param7:args.value7,
     args.param8:args.value8,
     args.param9:args.value9,
     args.param10:args.value10
}
# print(args.value1)
# print(params)
context = dataobjects.createContext()
context.setInvalidGeometryCheck(QgsFeatureRequest.GeometryNoCheck)
res = processing.run(args.alg, params, context=context)
