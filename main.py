import os
import sys
from xml.etree import ElementTree as ET
from tqdm import tqdm
from src.data import xml_extraction as xmle

if __name__ == '__main__':
    pageXMLLocation = sys.argv[1]
    out_path = sys.argv[2]
    print(sys.argv)
    directory = os.fsencode(pageXMLLocation)
    xmlroots = []

    print(f"Getting xml roots from {directory}")
    for file in tqdm(os.listdir(directory)):
        fileName = os.fsdecode(file)
        tree = ET.parse(os.path.join(pageXMLLocation, fileName))
        root = tree.getroot()
        xmlroots.append(root)

    print("Extrating catalogue entries from xmls")
    currentVolume = xmlroots
    allLines = xmle.extractLinesForVol(currentVolume)
    allLines = [line for line in allLines if line is not None]
    titles, allTitleIndices = xmle.findHeadings(allLines)

    print(f"Saving catalogue entries to {out_path}")
    xmle.saveAll(
        currentVolume=currentVolume,
        directory=directory,
        allTitleIndices=allTitleIndices,
        allLines=allLines,
        path=out_path
    )

    #savePoorlyScannedPages(getPoorlyScannedPages(currentVolume, os.listdir(directory)))
    #saveRawTxt(allTitleIndices, allLines)
    #saveSplitTxt(allTitleIndices, allLines)
    #saveXML(allTitleIndices, allLines)

    # discrepancy between number of titles and output files

