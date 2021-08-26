# Ambient_populations

Analysis of the Leeds City Council Footfall Camera dataset.

Analysis completed so far:

- Initial analysis can be found under analysis/Footfall_Camera_Analysis.ipynb
- Inspection of data quality for time series modelling, including decomposing the components and exploring some missing data
- Framework for Random Forest Modelling has been created under analysis/RandomForest_Footfall.ipynb
- Initial modelling using Facebook Prophet can be found under analysis/FBProphet_modelling.ipynb (This is very basic and was only set up a few days ago)

The notebooks require source.py and data/LCC_footfall_2021.gz to run correctly.

There are several .py scripts in the root folder that undertake a number of functions:

- source.py contains a lot of custom functions called by the analysis notebooks.
- footfall_data_download.py
