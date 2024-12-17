import os
from src.data import xml_extraction as xmle

CSV = True
RAW_TEXT = True
SPLIT_TEXT = True

if __name__ == '__main__':

    for i in range(1, 11):
        print(f"Vol {i}: finding xmls at 2/4 column locations")
        two_col_loc = f"data\\raw\\BMC_{i}_2\\*\\*.pxml"
        four_col_loc = f"data\\raw\\BMC_{i}_4\\*\\*.pxml"
        xmls_2, xmls_4 = xmle.gen_xml_paths(two_col_loc), xmle.gen_xml_paths(four_col_loc)
        print(f"Extracting {len(xmls_2)} 2 col xmls and {len(xmls_4)} 4 col xmls for {len(xmls_2) + len(xmls_4)} total")
        vol_xml_trees = xmle.gen_xml_trees(xmls_2 + xmls_4)

        print("\nExtracting catalogue entries from xmls")
        lines, xml_track_df = xmle.extract_lines_for_vol(vol_xml_trees)
        title_shelfmarks, title_indices, ordered_lines = xmle.find_headings(lines)
        entry_df = xmle.extract_catalogue_entries(ordered_lines, title_indices, title_shelfmarks, xml_track_df)
        print(f"Extracted {len(entry_df)} entries")

        out_path = f"data\\processed\\BMC_{i}"
        print(f"Saving catalogue entries to {out_path}")
        if not os.path.exists(out_path):
            os.makedirs(out_path)

        if CSV:
            entry_df.to_csv(os.path.join(out_path, "catalogue_entries_v1.4.csv"), index=False, encoding="utf-8-sig")

        if RAW_TEXT:
            print("Saving raw txt files")
            with open(os.path.join(out_path, f"BMC_{i}_full_text_single_line_v1.4.txt"), "w", encoding="utf-8") as f:
                entry_df.apply(lambda x: f.write(" ".join(x["entry"]) + "\n"), axis=1)

        if SPLIT_TEXT:
            print("Extracting split text")
            entry_df["en_only"], entry_df["non_en_only"] = xmle.get_language_sections(entry_df["entry"])
            print("Saving split text")
            entry_df.to_csv(os.path.join(out_path, "catalogue_entries_split_langs_v1.4.csv"), index=False, encoding="utf-8-sig")
            with open(os.path.join(out_path, f"BMC_{i}_en_only_single_line_v1.4.txt"), "w", encoding="utf-8") as f:
                entry_df["en_only"].apply(lambda x: f.write(" ".join(x) + "\n"))
            with open(os.path.join(out_path, f"BMC_{i}_non_en_single_line_v1.4.txt"), "w", encoding="utf-8") as f:
                entry_df["non_en_only"].apply(lambda x: f.write(" ".join(x) + "\n"))

        # xmle.save_poorly_scanned_pages(xmle.get_poorly_scanned_pages(current_volume, xmls), out_path)
        # xmle.save_xml(lines, title_indices, title_shelfmarks, out_path)