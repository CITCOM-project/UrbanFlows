# Urban Flows (UF) / CITCOM Workspace

This repo is a workspace for Causal Inference work relating to data from the Sheffield [Urban Flows Observatory](https://urbanflows.ac.uk/)

## Useful Links

[Urban Flows Map Portal](https://sheffield-portal.urbanflows.ac.uk/uflobin/sufoPortal)

[Urban Flows Data Extraction Tool](https://sheffield-portal.urbanflows.ac.uk/uflobin/sufoDXT)

[Data Extraction Tool PDF Guide](https://github.com/CITCOM-project/UrbanFlows/blob/main/ufDXT_Guide.pdf) (Download PDF for hyperlinks to work)

## Loading Single Urban Flows CSV

1. Import the `LoadCsvUf` class from `uf_csv_loader.py`
2. Initialise class with path to the csv
3. Choose method relating to how you want to transform csv.

To see running example run
`python uf_data_scripts/uf_csv_loader.py` from the project root directory
