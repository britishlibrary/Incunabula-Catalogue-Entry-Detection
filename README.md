# Catalogue Entry Detection

This project investigates and implements different methods for detecting catalogue entries within printed catalogues. While printed catalogues are easy enough to digitise and convert into machine readable data, dividing that data by catalogue entry requires converting visual signifiers of divisions between entries - gaps in the printed page, large or upper-case headers, catalogue references - into machine-readable information.

The data used is XML-formatted data derived from the 13-volume *Catalogue of books printed in the 15th century now at the British Museum*. The project was undertaken in support of [Rossitza Atanassova](https://www.bl.uk/people/experts/rossitza-atanassova)'s [AHRC-RLUK Professional Practice Fellowship](https://blogs.bl.uk/digital-scholarship/2022/11/my-ahrc-rluk-professional-practice-fellowship-phase-one.html).

This project is the British Library maintained version of code produced in 2022/2023 by [Isaac Dunford](https://github.com/Mr-Esweg) as part of a [Digital Humanities](http://digitalhumanities.soton.ac.uk/) Internship funded by the School of Humanities at the University of Southampton. Isaac's original code is at https://github.com/Southampton-Digital-Humanities/2023_Catalogue-Entry-Detection.

Isaac describes the work in [his post of the British Library Digital Scholarship blog](https://blogs.bl.uk/digital-scholarship/2023/05/detecting-catalogue-entries-in-printed-catalogue-data.html).

## Project Organization

```
├── LICENSE            <- MIT License
├── README.md          <- The top-level README for developers using this project.
├── data
│   ├── external        <- Datasets from outside the project.
│   ├── interim        <- Intermediate data that has been transformed.
│   ├── processed      <- The final data sets.
│   └── raw            <- The original, immutable data dump.
│
├── reports             <- Figures and reports produced during the project
├── pyproject.toml     <- Project configuration file with package metadata for 
│                         emp and configuration for tools like black
│
├── uv.lock   <- The uv lock file for reproducing the analysis environment
|
├── main.py   <- Run code from src.data.xml_extraction
|
├── tests        <- pytest tests for the repository `uv run pytest tests/`
│
└── src        <- Source code for use in this project.
    └── union_lists
        ├── __init__.py             <- Makes union_lists a Python module
        │
        ├── config.py               <- Store useful variables and configuration
        |
        └── dataset
            │
            ├── __init__.py             <- Makes dataset a Python module
            |
            ├── extract_from_doc.py              <- Extract references from docx files
            ├── extract_from_xlsx.py              <- Extract references from xlsx files
            └── reformat_union_lists.py              <- Reformat references into new data model data from docx files
```

## Install
Copy the repository with `git clone`, then create the environment.
  
### uv
uv will sync the environment for you the first time you try to run code using 
```
$ uv run python main.py
```

however you can use `uv sync` if you want to set the environment up before you run it.

## Use
You can get a copy of the raw data from the Transkribus collection maintained by Rossitza Atanassova. This will include Volumes 1-10 of the British Museum Catalogue (BMC), volume 11 for the field-model work is in a separate Transkribus collection that Jeanette Croen ran. Each volume has data extracted using a 2 column and 4 column model. RA can explain the difference between the two models.

Save the Transkribus output XMLs in subfolders of the raw data folder in the format `BMC_<vol>_<n_cols>`. So one folder for BMC_1_2 (vol 1, 2 col model), one for BMC_1_4 etc.

### Extract catalogue entries from raw XMLs
All the code is called through `main.py`, so run
```
$ uv run python main.py
```

#### Outputs
Outputs will go to `data/processed/BMC_<vol>`, and consist of up to 5 xlsx files, depending on whether catalogue entries have been separated by language or not (using the `SPLIT_TEXT` switch).
- `BMC_<vol>/catalogue_entries_v1.4.csv`: all extracted catalogue entries for that volume
- `BMC_<vol>/BMC_<vol>_full_text_single_line_v1.txt`: text for each catalogue entry, each set on a new line of text document, helpful for corpus linguistics software like AntConc

If splitting text by language:
- `BMC_<vol>/catalogue_entries_split_langs_v1.4.csv`: catalogue entries for the volume with only the english text included in the entries
- `BMC_<vol>/BMC_<vol>_en_only_single_line_v1.txt`: only the english text for each catalogue entry, each set on a new line
- `BMC_<vol>/BMC_<vol>_non_en_single_line_v1.txt`: only the non-english text for each catalogue entry, each set on a new line

### Match extracted shelfmarks to the complete BLL01 list of shelfmarks
Once you've run main.py to produce the catalogue_entries_v1.4 csvs for each volume you can combine them to check the extracted entry shelfmarks against our complete list of BL shelfmark.

Run through `notebooks/bll01_matching_records.ipynb` to do this, a version of this output is stored in the network drive folder Incunabula Catalogue Entry Detection (Harry) as `incu_ocr_bll_matching_v1.4.xlsx`.

### Tests
`uv run pytest tests/` will run the test suite. Coverage is reasonable for small test cases of extracting entries from XML but not for much else.

--------
Code was developed by Harry Lloyd, Research Software Engineer, British Library. Contact harry.lloyd [at] bl.uk.

## License

All data provided by the [British Library](https://creativecommons.org/licenses/by/4.0/): text data [CC0 1.0 Universal Public Domain](https://creativecommons.org/publicdomain/zero/1.0/); images [CC-BY 4.0 International](https://creativecommons.org/licenses/by/4.0/). For code use [MIT License](https://mit-license.org/).
