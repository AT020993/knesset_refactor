"""
Performance optimization utilities for Streamlit app.

This module provides utilities to optimize data processing and rendering
for better performance on limited resources (e.g., Streamlit Cloud free tier with 1GB RAM).
"""

import logging
from typing import Callable, Optional

import pandas as pd


def optimize_dataframe_for_display(
    df: pd.DataFrame,
    max_rows: int = 10000,
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Optimize a dataframe for display by reducing size if needed.

    Args:
        df: Input dataframe
        max_rows: Maximum number of rows to display
        logger: Optional logger for info messages

    Returns:
        Optimized dataframe
    """
    if df.empty or len(df) <= max_rows:
        return df

    if logger:
        logger.info(f"Dataframe has {len(df)} rows, sampling to {max_rows} for display")

    # Sample evenly across the dataset
    step = len(df) // max_rows
    return df.iloc[::step].reset_index(drop=True)


def downsample_timeseries(
    df: pd.DataFrame,
    time_column: str,
    value_column: str,
    max_points: int = 500,
    aggregation_func: str = 'sum',
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Downsample time series data to reduce number of points for plotting.

    Args:
        df: Input dataframe with time series data
        time_column: Name of the time/period column
        value_column: Name of the value column to aggregate
        max_points: Maximum number of data points to keep
        aggregation_func: Aggregation function ('sum', 'mean', 'max', 'min')
        logger: Optional logger for info messages

    Returns:
        Downsampled dataframe
    """
    if df.empty or len(df[time_column].unique()) <= max_points:
        return df

    if logger:
        logger.info(f"Downsampling time series from {len(df[time_column].unique())} to {max_points} points")

    # Calculate how many original points to group together
    n_unique = len(df[time_column].unique())
    group_size = max(1, n_unique // max_points)

    # Sort by time
    df_sorted = df.sort_values(time_column)

    # Group and aggregate
    agg_dict = {value_column: aggregation_func}

    # If there are other columns, aggregate them as well
    for col in df.columns:
        if col not in [time_column, value_column]:
            if df[col].dtype == 'object':
                agg_dict[col] = 'first'
            else:
                agg_dict[col] = 'mean'

    # Create groups
    df_sorted['_group'] = df_sorted.groupby(time_column).ngroup() // group_size

    result = df_sorted.groupby(['_group', time_column]).agg(agg_dict).reset_index()
    result = result.drop('_group', axis=1)

    return result


def reduce_plotly_figure_size(
    fig,
    simplify_traces: bool = True,
    logger: Optional[logging.Logger] = None
):
    """
    Optimize a Plotly figure for faster rendering by reducing data complexity.

    Args:
        fig: Plotly figure object
        simplify_traces: Whether to simplify trace data
        logger: Optional logger for info messages

    Returns:
        Optimized figure
    """
    if not fig:
        return fig

    # Reduce marker size for scatter plots with many points
    for trace in fig.data:
        if hasattr(trace, 'marker') and hasattr(trace, 'x'):
            if len(trace.x) > 1000:
                if logger:
                    logger.info(f"Optimizing trace with {len(trace.x)} points")

                # Reduce marker size for performance
                if trace.marker.size is not None and trace.marker.size > 5:
                    trace.marker.size = 5

                # Disable marker lines for better performance
                if hasattr(trace.marker, 'line'):
                    trace.marker.line.width = 0

    # Optimize layout for performance
    fig.update_layout(
        # Disable hover label on hover for better performance with large datasets
        hovermode='closest',
        # Reduce animation duration
        transition_duration=300,
        # Optimize legend
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.01
        )
    )

    return fig


def estimate_dataframe_memory(df: pd.DataFrame) -> dict:
    """
    Estimate memory usage of a dataframe.

    Args:
        df: Input dataframe

    Returns:
        Dictionary with memory usage statistics
    """
    memory_bytes = df.memory_usage(deep=True).sum()

    return {
        'rows': len(df),
        'columns': len(df.columns),
        'memory_bytes': memory_bytes,
        'memory_mb': memory_bytes / (1024 * 1024),
        'memory_per_row_kb': (memory_bytes / len(df) / 1024) if len(df) > 0 else 0
    }


def optimize_dataframe_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimize dataframe data types to reduce memory usage.

    Args:
        df: Input dataframe

    Returns:
        Optimized dataframe with smaller dtypes
    """
    if df.empty:
        return df

    df_optimized = df.copy()

    for col in df_optimized.columns:
        col_type = df_optimized[col].dtype

        # Optimize integer columns
        if col_type == 'int64':
            col_min = df_optimized[col].min()
            col_max = df_optimized[col].max()

            if col_min >= 0:
                if col_max < 255:
                    df_optimized[col] = df_optimized[col].astype('uint8')
                elif col_max < 65535:
                    df_optimized[col] = df_optimized[col].astype('uint16')
                elif col_max < 4294967295:
                    df_optimized[col] = df_optimized[col].astype('uint32')
            else:
                if col_min > -128 and col_max < 127:
                    df_optimized[col] = df_optimized[col].astype('int8')
                elif col_min > -32768 and col_max < 32767:
                    df_optimized[col] = df_optimized[col].astype('int16')
                elif col_min > -2147483648 and col_max < 2147483647:
                    df_optimized[col] = df_optimized[col].astype('int32')

        # Optimize float columns
        elif col_type == 'float64':
            df_optimized[col] = df_optimized[col].astype('float32')

        # Convert object columns to category if beneficial
        elif col_type == 'object':
            num_unique = len(df_optimized[col].unique())
            num_total = len(df_optimized[col])

            # If less than 50% unique values, use category
            if num_unique / num_total < 0.5:
                df_optimized[col] = df_optimized[col].astype('category')

    return df_optimized


def batch_process_large_query(
    query_func: Callable[..., pd.DataFrame],
    batch_size: int = 10000,
    logger: Optional[logging.Logger] = None,
    **kwargs
) -> pd.DataFrame:
    """
    Process large queries in batches to avoid memory issues.

    Args:
        query_func: Function that executes the query and returns a dataframe
        batch_size: Number of rows to process per batch
        logger: Optional logger for progress messages
        **kwargs: Additional arguments to pass to query_func

    Returns:
        Combined dataframe from all batches
    """
    results: list[pd.DataFrame] = []
    offset = 0

    while True:
        if logger:
            logger.info(f"Processing batch starting at offset {offset}")

        # Add LIMIT and OFFSET to kwargs
        kwargs['limit'] = batch_size
        kwargs['offset'] = offset

        batch_df = query_func(**kwargs)
        if not isinstance(batch_df, pd.DataFrame):
            if logger:
                logger.warning("Batch query function returned non-DataFrame result; stopping batch processing.")
            break

        if batch_df.empty:
            break

        results.append(batch_df)

        if len(batch_df) < batch_size:
            # Last batch
            break

        offset += batch_size

    if not results:
        return pd.DataFrame()

    return pd.concat(results, ignore_index=True)
