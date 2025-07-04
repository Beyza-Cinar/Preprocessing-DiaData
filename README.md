# Preprocessing-DiaData

This repository presents data cleaning, and pre-processing steps of DiaData, a large integrated CGM Dataset of subjects with Type 1 Diabetes presented in: https://github.com/Beyza-Cinar/DiaData. Detailed instruction on acquiring the single datasets and restricted datasets is provided in https://github.com/Beyza-Cinar/DiaData as well. The raw datasets can be downloaded from: https://openhsu.ub.hsu-hh.de/handle/10.24405/20048.

The steps include:
1. Data cleaning with outlier detection using the interquartile range (IQR) method.
2. Treatment of missing values using linear interpolation for data gaps smaller than 30 minutes and Stineman interpolation for data gaps between 30 and 120 minutes.
3. Data pre-processing for hypoglycemia detection involving class assignment, data normalization with min-max scaling, time series generation of 2 hour windows, and class balancing. Five classes were defined, corresponding to 0, 5–14, 15–29, 30–59, and 60–120 minutes before hypoglycemia.
4. Data analysis involving correlation analysis on a subset of DiaData including glucose and heart rate measurements.
5. Benchmarking on DiaData with hypoglycemia classification up to 2 hours before onset.

## Requirements

python version 3.11.4

matplotlib version 3.7.2

pandas version 2.0.3

numpy version 1.24.3

polars version 1.27.1

TensorFlow version 2.13.0

## Code Organization

The code is organized as follows:

- data_integration.py: Contains data intergation steps since data is integrated again because missing values produced via undersampling are not treated with feedforward filling in this study.
- data_preprocessing.py: Contains the functions for preprocessing the dataset for an improved quality and the benchmarking task.
- data_pre-processing_MDB.ipynb: Contains the code for pre-processing and model training on the maindabase of DiaData including all subjects.
- data_pre-processing_MDB_raw.ipynb: Contains the code for pre-processing and model training on the raw maindabase of DiaData including all subjects.
- data_cleaning_SBI.ipynb: Contains the code for data cleaning on Subdatabase I of DiaData including only subjects with available glucose values and demographics.
- data_pre-processing_SBII.ipynb: Contains the code for pre-processing and model training on Subdatabase II of DiaData including only subjects with available glucose and heart rate data.
- data_pre-processing_SBII_raw.ipynb: Contains the code for pre-processing and model training on th raw Subdatabase II of DiaData including only subjects with available glucose and heart rate data.
