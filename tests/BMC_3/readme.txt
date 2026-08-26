Export of:
lines, xml_track_df = xmle.extract_lines_for_vol(vol_xml_trees)
with open("..\\tests\\BMC_3\\lines.txt", "w", encoding="utf-8") as f:
	[f.write(l + "\n") for l in lines]