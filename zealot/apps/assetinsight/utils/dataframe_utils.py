"""
DataFrame utilities for PyArrow compatibility
Ensures all DataFrames are properly typed for Streamlit display
"""

import pandas as pd
import numpy as np
from typing import Any, Dict, List, Union


def make_arrow_compatible(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make a DataFrame PyArrow-compatible by ensuring proper data types.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with PyArrow-compatible data types
    """
    if df.empty:
        return df
    
    df_clean = df.copy()
    
    for col in df_clean.columns:
        # Get the column data
        col_data = df_clean[col]
        
        # Skip if already properly typed
        if col_data.dtype in ['int64', 'float64', 'bool', 'string']:
            continue
            
        # Handle object columns (most problematic for PyArrow)
        if col_data.dtype == 'object':
            # Try to convert to numeric first
            numeric_converted = pd.to_numeric(col_data, errors='coerce')
            
            # If all values converted successfully to numeric, use numeric
            if not numeric_converted.isna().any():
                df_clean[col] = numeric_converted
                continue
            
            # If some values converted, use numeric with NaN fill
            if not numeric_converted.isna().all():
                df_clean[col] = numeric_converted.fillna(0)
                continue
            
            # Otherwise, convert everything to string
            df_clean[col] = col_data.astype(str)
            continue
        
        # Handle other problematic types
        if col_data.dtype in ['int32', 'int16', 'int8', 'float32', 'float16']:
            # Convert to standard numeric types
            if col_data.dtype in ['int32', 'int16', 'int8']:
                df_clean[col] = col_data.astype('int64')
            else:
                df_clean[col] = col_data.astype('float64')
            continue
        
        # Handle datetime columns
        if 'datetime' in str(col_data.dtype):
            df_clean[col] = col_data.dt.strftime('%Y-%m-%d %H:%M:%S')
            continue
        
        # Handle boolean columns
        if col_data.dtype == 'bool':
            df_clean[col] = col_data.astype('bool')
            continue
        
        # Default: convert to string
        df_clean[col] = col_data.astype(str)
    
    return df_clean


def safe_dataframe(data: Union[List[Dict], Dict, pd.DataFrame], **kwargs) -> pd.DataFrame:
    """
    Create a PyArrow-compatible DataFrame from various data sources.
    
    Args:
        data: Data to convert to DataFrame (list of dicts, dict, or DataFrame)
        **kwargs: Additional arguments passed to pd.DataFrame
        
    Returns:
        PyArrow-compatible DataFrame
    """
    # Create DataFrame
    if isinstance(data, pd.DataFrame):
        df = data.copy()
    else:
        df = pd.DataFrame(data, **kwargs)
    
    # Make it PyArrow-compatible
    return make_arrow_compatible(df)


def clean_numeric_column(series: pd.Series, fill_na: Any = 0) -> pd.Series:
    """
    Clean a numeric column to be PyArrow-compatible.
    
    Args:
        series: Input pandas Series
        fill_na: Value to fill NaN values with
        
    Returns:
        Cleaned numeric Series
    """
    # Convert to numeric, coercing errors to NaN
    numeric_series = pd.to_numeric(series, errors='coerce')
    
    # Fill NaN values
    if fill_na is not None:
        numeric_series = numeric_series.fillna(fill_na)
    
    # Convert to int64 if all values are whole numbers
    if numeric_series.dtype == 'float64':
        if (numeric_series % 1 == 0).all():
            return numeric_series.astype('int64')
    
    return numeric_series.astype('float64')


def clean_string_column(series: pd.Series, fill_na: str = '') -> pd.Series:
    """
    Clean a string column to be PyArrow-compatible.
    
    Args:
        series: Input pandas Series
        fill_na: Value to fill NaN values with
        
    Returns:
        Cleaned string Series
    """
    # Convert to string, handling NaN values
    string_series = series.astype(str)
    
    # Replace 'nan' strings with fill_na
    string_series = string_series.replace('nan', fill_na)
    
    return string_series.astype('string')


def validate_dataframe_for_streamlit(df: pd.DataFrame) -> bool:
    """
    Validate if a DataFrame is PyArrow-compatible for Streamlit.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        True if compatible, False otherwise
    """
    try:
        # Try to convert to Arrow table (this is what Streamlit does internally)
        import pyarrow as pa
        pa.Table.from_pandas(df)
        return True
    except Exception:
        return False
