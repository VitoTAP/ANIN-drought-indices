# Drought indices

Source code for the drought indices used in the ANIN project. Download without registering by clicking `<> Code` -> `Download ZIP`

This contains an openEO implementation of the algorithms that where initially developed by GMV.

https://eoafrica.drought-za.org/

## Installation:

Works with standard [Python](https://www.python.org/downloads/). Be sure to [add Python to PATH](https://realpython.com/add-python-to-path/)

Also works with [CONDA](https://docs.anaconda.com/free/miniconda/)

Register for openeo: https://docs.openeo.cloud/join/free_trial.html
A free account should be more than enough. if it does not suffice, there are many credits available for this project. 
Ask emile.sonneveld@vito.be to link them to your account.

[Example output files can be found on artifactory](https://artifactory.vgt.vito.be/artifactory/auxdata-public/ANIN/).

```bash
git clone https://github.com/VitoTAP/ANIN-drought-indices
cd ANIN-drought-indices
python -m pip install -r requirements.txt
```

To install on CONDA: 
```bash
conda config --append channels conda-forge
conda install --yes --file requirements.txt
```

More instructions here: https://open-eo.github.io/openeo-python-client/installation.html

## Running:

```bash
python -m SMA.SMA_openeo
python -m SPI.SPI_openeo
python -m SPEI.SPEI_openeo
python -m VCI.VCI_openeo
python -m FAPAR_Anomaly.FAPAR_Anomaly_openeo
```

For every layer you can overwrite the temporal extent. For CDI it is required for performance:
```bash
python -m CDI.CDI_openeo "2001-01-01" "2023-01-01"
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

