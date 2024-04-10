# Drought indices

## Installation:

```bash
git clone https://github.com/VitoTAP/ANIN-drought-indices
cd ANIN-drought-indices
python3 -m pip install -r requirements.txt
```

To install on CONDA: `conda install --yes --file requirements.txt`

More instructions here: https://open-eo.github.io/openeo-python-client/installation.html

## Running:

```bash
python3 -m SMA.SMA_openeo
python3 -m SPI.SPI_openeo
python3 -m SPEI.SPEI_openeo
python3 -m VCI.VCI_openeo
python3 -m FAPAR_Anomaly.FAPAR_Anomaly_openeo
```

For every layer you can overwrite the temporal extent. For CDI it is required for performance:
```bash
python3 -m CDI.CDI_openeo "2020-01-01" "2023-01-01"
```

Checkout the notebook examples:
```
SPI/SPI_openeo.ipynb
SMA/SMA_openeo.ipynb
```

## Notes

Format code:
```bash
black --line-length 120 */*_openeo.py */*_UDF.py
```

Example output files can be found here: https://vitoresearch-my.sharepoint.com/:f:/g/personal/emile_sonneveld_vito_be/EoYTLvl14rdCoH1WDKvKwygBGCZdhMNfTBxnaZFpImBzUQ?e=wJfq37
