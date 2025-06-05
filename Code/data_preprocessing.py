# imports
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.model_selection import train_test_split
import numpy as np 
import datetime
import polars as pl


def remove_outliers(df_org, value, modus, subject = "PtID"):
    """
    This function is used to identify and remove outliers with the interquartile range method.
    Parameters:
   - df_org is the original dataset, 
   - value is the name of the column with the values of interest containing outliers,
   - the modus—either "glucose" or "vitals"—specifies the requirements for outlier removal,
   - subject is the subject IDfillid is the name of the column which needs fillforward to impute produced missing values.
    Output: It returns the database with removed outliers in the specified column.
   """ 
    data = df_org.copy()
    # repaces zero values with nan values
    data[value].replace(0, np.nan, inplace=True) 
    # computes group-wise statistics, identifying the first and third quartiles
    Q1 = data.groupby(subject)[value].transform(lambda x: x.quantile(0.25))
    Q3 = data.groupby(subject)[value].transform(lambda x: x.quantile(0.75))
    IQR = Q3 - Q1

    # computes lower and upper thresholds
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR

    if modus == "glucose":
      # identifies outliers which are not in the range of 25 and 75 percent of values and replaces them with nan values
      # also removes CGM values which are less than 40 mg/dL
      is_outlier = (data[value] < lower) | (data[value] > upper) | (data[value] < 40) | (data[value] > 500) # maybe also larger than 500
    elif modus == "vitals":
      # identifies outliers which are not in the range of 25 and 75 percent of values and replaces them with nan values
      # also removes CGM values which are less than 40 mg/dL
      is_outlier = (data[value] < lower) | (data[value] > upper) | (data[value] < 30) # maybe also larger than 500
    else:
      print("modus must be glucose or vitals")

    # outliers in the value column are replaced with nan values
    data.loc[is_outlier, value] = np.nan

    return data


def gap_limited_interpolation(df_filtered, limit=6):
    """
    This function is used to impute missing values with linear interpolation based on a sepcified gap length.
    Parameters:
   - df_filtered is the original series of a defined column, 
   - limit is the number of consecutive values which should be imputed.
    Output: It returns a series of the specified column with linearly imputed values.
   """ 
    # creates a group ID for consecutive NaNs
    is_nan = df_filtered.isna()
    not_nan = ~is_nan
    group_id = (not_nan != not_nan.shift()).cumsum()

    # gets all groups that are NaNs and their sizes
    nan_groups = is_nan.groupby(group_id).transform('sum')

    # masks values to interpolate: NaNs and in small enough groups
    interpolate_mask = is_nan & (nan_groups < limit)

    # applies linear interpolation
    ts_interp = df_filtered.copy()
    ts_interp[interpolate_mask] = df_filtered.interpolate(method='linear')[interpolate_mask]
    return ts_interp


# used for stineman interpolation
# source: https://github.com/jdh2358/py4science/blob/master/examples/extras/steinman_interp.py
def slopes(x, y):
    """
    Estimate the derivative y'(x) using a parabolic fit through three consecutive points.
    
    This method approximates slopes based on local parabolas and is numerically robust 
    for functions where abscissae (x) and ordinates (y) may differ in scale or units.

    Originally described by Norbert Nemec (2006), inspired by Halldor Bjornsson.
    """

    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)

    if len(x) < 2 or len(y) < 2:
        return np.full_like(y, np.nan)

    yp = np.full_like(y, np.nan)

    dx = x[1:] - x[:-1]
    dy = y[1:] - y[:-1]

    with np.errstate(divide='ignore', invalid='ignore'):
        dydx = np.divide(dy, dx, out=np.zeros_like(dy), where=dx != 0)

    denom = dx[1:] + dx[:-1]
    valid = denom != 0

    numerator = dydx[:-1] * dx[1:] + dydx[1:] * dx[:-1]
    with np.errstate(divide='ignore', invalid='ignore'):
        result = np.where(np.isfinite(denom) & (denom != 0), numerator / denom, 0.0)
    yp[1:-1][valid] = result[valid]

    yp[0] = 2 * dydx[0] - yp[1] if not np.isnan(yp[1]) else np.nan
    yp[-1] = 2 * dydx[-1] - yp[-2] if not np.isnan(yp[-2]) else np.nan

    return yp

# Stineman interpolation 
# original source: https://github.com/jdh2358/py4science/blob/master/examples/extras/steinman_interp.py
def stineman_interp(xi, x, y, yp=None):
    """
    Perform Stineman interpolation on known data points (x, y) at new locations xi.
    
    If derivative estimates yp are not supplied, they are estimated using `slopes(x, y)`.

    Returns interpolated values yi corresponding to xi.
    """
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    xi = np.asarray(xi, dtype=float)

    if yp is None:
        yp = slopes(x, y)
    else:
        yp = np.asarray(yp, dtype=float)

    if len(x) < 2:
        return np.full_like(xi, np.nan)

    dx = x[1:] - x[:-1]
    dy = y[1:] - y[:-1]

    with np.errstate(divide='ignore', invalid='ignore'):
        s = np.divide(dy, dx, out=np.zeros_like(dy), where=dx != 0)

    idx = np.searchsorted(x[1:-1], xi)
    # protect out-of-bounds
    idx = np.clip(idx, 0, len(x) - 2)  

    sidx = s[idx]
    xidx = x[idx]
    yidx = y[idx]
    xidxp1 = x[idx + 1]

    yo = yidx + sidx * (xi - xidx)
    dy1 = (yp[idx] - sidx) * (xi - xidx)
    dy2 = (yp[idx + 1] - sidx) * (xi - xidxp1)
    dy1dy2 = dy1 * dy2

    dy1dy2_clean = np.nan_to_num(dy1dy2, nan=0.0, posinf=0.0, neginf=0.0)
    cond = np.sign(dy1dy2_clean).astype(int) + 1
    

    with np.errstate(divide='ignore', invalid='ignore'):
        denom_0 = (dy1 - dy2) * (xidxp1 - xidx)
        denom_2 = dy1 + dy2

        blend_0 = np.divide((2 * xi - xidx - xidxp1), denom_0,
                            out=np.zeros_like(dy1), where=denom_0 != 0)
        blend_2 = np.divide(1.0, denom_2,
                            out=np.zeros_like(dy1), where=denom_2 != 0)

        blend = np.choose(cond, [blend_0, np.zeros_like(dy1), blend_2])

    yi = yo + dy1dy2 * blend
    # converts any remaining inf to nan
    yi[np.isinf(yi)] = np.nan  

    return yi


# gap-limited Stineman interpolation
def interpolate_stineman_group(df_org, timestamp, value, llimit=6, ulimit=24, yp=None):
    """
    This function is used to impute missing values with Stineman interpolation based on a sepcified gap length.
    Parameters:
   - df_org is the original dataframe, 
   - timestamp is the name of the column with the timestamps, 
   - value is the name of the column which should be interpolated
   - llimit is the lower limit of the gap length
   - ulimit is the upper limit of the gap length
    Output: It returns the interpolated dataframe of the specified column with Stineman imputed values.
   """ 
    
    group = df_org.copy()
    group = group.sort_values(timestamp).copy()
    tf = (group[timestamp] - group[timestamp].min()).dt.total_seconds()
    val = group[value].copy()
    is_nan = val.isna().to_numpy()    

    # constraint
    if (~is_nan).sum() < 2:
        group[f'{value}_interp'] = val
        return group

    # labels NaN runs
    run_id = (is_nan != np.roll(is_nan, 1)).cumsum()
    run_id[0] = 1

    # computes run lengths
    run_lengths = pd.Series(is_nan).groupby(run_id).transform('sum').to_numpy()

    # gets run IDs that are fully eligible (between 4–12 NaNs only)
    run_id_series = pd.Series(run_id, index=val.index)
    valid_run_ids = run_id_series[(is_nan) & (run_lengths >= llimit) & (run_lengths < ulimit)].unique()

    # masks only those full runs
    mask = run_id_series.isin(valid_run_ids) & is_nan

    # interpolates with Stineman interpolation
    x_known = tf[~is_nan]
    y_known = val[~is_nan]
    x_interp = tf[mask]

    if x_interp.size > 0:
        y_interp = stineman_interp(x_interp, x_known, y_known)
        val.iloc[mask] = y_interp

    # rounds the output 
    group['value_interp'] = val.round(2)  

    # drops original columns
    group.drop(columns=[value], inplace=True)

    # renames the interpolated column
    group.rename(columns={'value_interp': value}, inplace=True)

    # returns the interpolated column
    return group



# generates classes for the hypoglycemia classification task
def class_generation(df_copy: pd.DataFrame, timestamp_col: str, start: int, end: int, class_number: int) -> pl.DataFrame: 
    """
    This function is used to assign labels.
    Parameters:
   - df_copy is the original pandas dataframe, 
   - timestamp_col is the name of the column with the timestamps, 
   - start is time in minutes of the minimum duration before hypogylcemia
   - end is time in minutes of the maximum duration before hypogylcemia
   - class_number is the class for the defined time range before hypoglycemia
    Output: It returns the dataframe with the assigned class for the spefied time range before hypoglycemia.
   """ 
    # first, the dataframe is converted to polars to increase efficacy
    df = pl.from_pandas(df_copy)
    # timestamps are sorted 
    df = df.sort(timestamp_col)
    
    # zero events are hypoglycemic data points
    # these are selected and the timestamps are stores as a series
    event_times = df.filter(pl.col("Class") == 0).select(timestamp_col).to_series()

    # the start and end time of the time range before hypogylcemia are computed
    start_bounds = event_times - datetime.timedelta(minutes=start)
    end_bounds = event_times - datetime.timedelta(minutes=end)

    # the series is masked 
    mask = pl.Series([False] * df.height)

    # timestamps meeting the defined criteria of specified time bonds are identified 
    # moreover, the class should be -1 (not assinged as other classes before)
    for start_time, end_time in zip(start_bounds, end_bounds):
        window_mask = (
            (df[timestamp_col] > end_time) &
            (df[timestamp_col] <= start_time) &
            (df["Class"] == -1)
        )
        mask = mask | window_mask

    df = df.with_columns([
        pl.when(mask).then(class_number).otherwise(pl.col("Class")).alias("Class")
    ])

    # the dataframe is converted back to pandas 
    df_pd = df.to_pandas()
    # the dataframe with assigned classes is returned
    return df_pd


# data is normalized with minmax scaling
def normalize_data(df, value):
    """
    This function min-max scales the values of the specified colum.
    Parameters: 
        - df is the original dataframe
        - value is the name of the column which should be normalized
    """
    # copies the data 
    df_min_max_scaled = df.copy() 
    column = [value]
    # applies normalization techniques (min max scaling)
    df_min_max_scaled[column] = (df_min_max_scaled[column] - df_min_max_scaled[column].min()) / (df_min_max_scaled[column].max() - df_min_max_scaled[column].min())	
    # returns the data which was min-max scaled 
    return df_min_max_scaled


# generates time series with a sliding window appraoch of 2 hour lengths for the maindatabase 
# splits data into train, validation, and test
def extract_valid_windows_GLC(
    df_org: pd.DataFrame,
    timestamp_col: str = "ts",
    feature_col: str = "GlucoseCGM",
    class_col: str = "Class",
    expected_sample_count: int = 25,
    min_window_duration = np.timedelta64(2, 'h')
):
    """
    This function is used genearte time series data and splits the data into train, validation, and test data.
    Parameters:
   - df_org is the original pandas dataframe, 
   - timestamp_col is the name of the column with the timestamps, 
   - feature_col is the name of the column which should be returned as a time series,
   - class_col is name of the column with the classes,
   - expected_sample_count is the number of allowed continuous values in a time series,
    - min_window_duration is the allowed continuous time of a time series
    Output: It returns a list including six separate list for X_train, X_val, X_test, Y_train, Y_val, and Y_test
   """ 
    # X_train, X_val, X_test, Y_train, Y_val, and Y_test are initialized as empty arrays
    X_train = []
    Y_train = []

    X_test = []
    Y_test = []

    X_val = []
    Y_val = []

    # dataframe is converted to polars for increased efficiency
    df = pl.from_pandas(df_org)
    # ensures datetime type and sort
    df = df.sort(timestamp_col)

    timestamps = df[timestamp_col].to_numpy()
    n_rows = df.height

    # empty arrays are initialized for the windows and labels
    windows = []
    labels = []

    start_idx = 0

    for end_idx in range(n_rows):
        while timestamps[end_idx] - timestamps[start_idx] > min_window_duration:
            start_idx += 1

        count = end_idx - start_idx + 1

        if (
            timestamps[end_idx] - timestamps[start_idx] >= min_window_duration and
            count == expected_sample_count
        ):
            window_df = df.slice(start_idx, count)

            nulls = window_df.null_count().row(0)
            if all(count == 0 for count in nulls):
                # gets label of last point in window
                label = window_df[class_col].to_list()[-1]  
                # only allows classes 0–4
                if label in {0, 1, 2, 3, 4}:  
                    windows.append(window_df[feature_col].to_list()) 
                    labels.append(label)


    if len(windows) > 0:
        windows = np.array(windows).reshape(-1, 1) 
        labels = np.array(labels).reshape(-1, 1)

        X_data = windows.reshape(-1, expected_sample_count, 1)
        Y_data = labels.reshape(-1, 1)

        # sequential split: train → val → test
        X_temp, X_test_subject, Y_temp, Y_test_subject = train_test_split(
            X_data, Y_data, test_size=0.15, shuffle=False
        )
        X_train_subject, X_val_subject, Y_train_subject, Y_val_subject = train_test_split(
            X_temp, Y_temp, test_size=0.1765, shuffle=False
        )

        X_train.append(X_train_subject)
        Y_train.append(Y_train_subject)

        X_val.append(X_val_subject)
        Y_val.append(Y_val_subject)

        X_test.append(X_test_subject)
        Y_test.append(Y_test_subject)
    
        return X_train, X_val, X_test, Y_train, Y_val, Y_test
    else:
        return 
    

# generates time series with a sliding window appraoch of 2 hour lengths for subdatabase II
# splits data into train, validation, and test
def extract_valid_windows_GLC_HR(
    df_org: pd.DataFrame,
    timestamp_col: str = "ts",
    feature_col: str = "GlucoseCGM",
    feature_col2: str = "HR",
    class_col: str = "Class",
    expected_sample_count: int = 25,
    min_window_duration = np.timedelta64(2, 'h')
):
    """
    This function is used genearte time series data and splits the data into train, validation, and test data.
    Parameters:
   - df_org is the original pandas dataframe, 
   - timestamp_col is the name of the column with the timestamps, 
   - feature_col is the name of the column which should be returned as a time series,
   - feature_col2 is the name of the second column which should be returned as a time series,
   - class_col is name of the column with the classes,
   - expected_sample_count is the number of allowed continuous values in a time series,
    - min_window_duration is the allowed continuous time of a time series
    Output: It returns a list including six separate list for X_train, X_val, X_test, Y_train, Y_val, and Y_test
   """ 
    
    X_train = []
    Y_train = []

    X_test = []
    Y_test = []

    X_val = []
    Y_val = []

    df = pl.from_pandas(df_org)
    # ensures datetime type and sort
    df = df.sort(timestamp_col)

    timestamps = df[timestamp_col].to_numpy()
    n_rows = df.height

    windows = []
    labels = []

    start_idx = 0

    for end_idx in range(n_rows):
        while timestamps[end_idx] - timestamps[start_idx] > min_window_duration:
            start_idx += 1

        count = end_idx - start_idx + 1

        if (
            timestamps[end_idx] - timestamps[start_idx] >= min_window_duration and
            count == expected_sample_count
        ):
            window_df = df.slice(start_idx, count)

            nulls = window_df.null_count().row(0)
            if all(count == 0 for count in nulls):
                # gets label of last point in window
                label = window_df[class_col].to_list()[-1]  
                # only allows classes 0–4
                if label in {0, 1, 2, 3, 4}:  
                    windows.append(
                        window_df.select([feature_col, feature_col2]).to_numpy().tolist()
                    )
                    labels.append(label)

    if len(windows) > 0:
        windows = np.array(windows).reshape(-1, 2) 
        labels = np.array(labels).reshape(-1, 1)

        X_data = windows.reshape(-1, expected_sample_count, 2)
        Y_data = labels.reshape(-1, 1)

        # sequential split: train → val → test
        X_temp, X_test_subject, Y_temp, Y_test_subject = train_test_split(
            X_data, Y_data, test_size=0.15, shuffle=False
        )
        X_train_subject, X_val_subject, Y_train_subject, Y_val_subject = train_test_split(
            X_temp, Y_temp, test_size=0.1765, shuffle=False
        )

        X_train.append(X_train_subject)
        Y_train.append(Y_train_subject)

        X_val.append(X_val_subject)
        Y_val.append(Y_val_subject)

        X_test.append(X_test_subject)
        Y_test.append(Y_test_subject)
    
        return X_train, X_val, X_test, Y_train, Y_val, Y_test
    else:
        return 
    


# flattens data 
def flatten_data(X, modus, axis_f = 1, shape_f = 25, dim = 1):
    """
    This function flattens the data returned from the time series generation function, since the window lists
    contain all windows of every subject.
    Parameters:
   - X is the original list, 
   - axis_f is the number of features, 
   - shape_f is the number of values in one window,
   - dim is the dimension,
    Output: It returns a the flattened array of the given list
   """ 
    array_data = [np.array(x) for x in X]  # convert all sublists to arrays
    flattened_data = np.concatenate(array_data, axis=axis_f)
    if modus == "input":
        flattened_data = flattened_data.reshape(-1,shape_f,dim)
    elif modus == "output":
        flattened_data = flattened_data.reshape(-1,dim)
    else:
        print("Modus must be either input or output.")
    print(flattened_data.shape) 
    return flattened_data