# Pre-processing_DiaData

This repository presents data cleaning, and pre-processing steps of DiaData, a large integrated CGM Dataset of subjects with Type 1 Diabetes presented in: https://github.com/Beyza-Cinar/DiaData.

The steps include:
1. Data cleaning with outlier detection using the interquartile range (IQR) method.
2. Treatment of missing values using linear interpolation for data gaps smaller than 30 minutes and Stineman interpolation for data gaps between 30 and 120 minutes.
3. Data pre-processing for hypoglycemia detection involving class assignment, data normalization with min-max scaling, time series generation of 2 hour windows, and class balancing. Five classes were defined, corresponding to 0, 5–14, 15–29, 30–59, and 60–120 minutes before hypoglycemia.
4. Data analysis involving correlation analysis on a subset of DiaData including glucose and heart rate measurements.
5. Benchmarking on DiaData with hypoglycemia classification up to 2 hours before onset.

## Code Organization

The code is organized as follows:

- data_cleaning.ipynb: Contains the code to clean the data
- data_pre-processing_MB.ipynb: Contains the code for pre-processing and model training on the maindabase of DiaData including all subjects. 
- data_pre-processing_SBII.ipynb: Contains the code for pre-processing and model training on Subdatabase II of DiaData including only subjects with available glucose and heart rate data. 
