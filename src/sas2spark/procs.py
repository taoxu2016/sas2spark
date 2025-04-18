# --- src/sas2spark/procs.py ---
from typing import Optional, List, Union, Tuple
import pandas as pd
from pandas.core.series import Series
from pandas.core.frame import DataFrame

# For PySpark support
try:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import functions as F
except ImportError:
    SparkDataFrame = None  # type: ignore
    F = None

def proc_freq(
    data: Union[DataFrame, "SparkDataFrame"],
    tables: List[str],
    weights: Optional[str] = None,
    noprint: bool = False,
) -> Union[
    Tuple[List[DataFrame], Optional[Series]],
    Tuple[List["SparkDataFrame"], Optional["SparkDataFrame"]],
]:
    """
    Mimics the functionality of SAS PROC FREQ for Pandas and PySpark DataFrames.

    Calculates frequency counts and percentages for categorical columns in a DataFrame.
    Supports optional weighting.

    Args:
        data: The input Pandas or PySpark DataFrame.
        tables: A list of column names to analyze. These columns should be categorical.
        weights: (Optional) The name of a column containing frequency weights.
        noprint: (Optional) If True, suppresses printing of the output. The function
            will still return the results.

    Returns:
        A tuple containing:
        - A list of DataFrames (Pandas) or SparkDataFrames, one for each table.
          Each DataFrame contains the frequency counts and percentages for the
          specified column. The columns in each output DataFrame are:
            - `[column_name]` (the analyzed column)
            - `Frequency` (the count of each unique value)
            - `Percent` (the percentage of each unique value)
            - `Weighted Frequency` (if `weights` is provided)
            - `Weighted Percent` (if `weights` is provided)
        - (Pandas only) A Pandas Series containing the overall frequency counts
          if `weights` is not None. For PySpark, this will be None.

    Raises:
        TypeError: If the input data is not a Pandas or PySpark DataFrame, or if
            `tables` is not a list.
        ValueError: If any of the columns specified in `tables` or `weights`
            do not exist in the DataFrame.
        ImportError: If PySpark is used but not installed.

    Examples:
        >>> import pandas as pd
        >>> data_pd = pd.DataFrame({'A': ['a', 'b', 'a', 'c', 'b', 'a'],
        ...                         'B': ['x', 'y', 'x', 'y', 'x', 'y'],
        ...                         'C': [1, 2, 1, 2, 1, 2],
        ...                         'W': [1, 2, 1, 2, 1, 2]})
        >>> result_pd, overall_pd = proc_freq(data_pd, tables=['A', 'B'], weights='W')
        >>> # Example output structure (actual print depends on noprint flag)

        >>> # Example with PySpark (requires PySpark installation)
        >>> # from pyspark.sql import SparkSession
        >>> # spark = SparkSession.builder.appName("ProcFreqExample").getOrCreate()
        >>> # data_spark = spark.createDataFrame(data_pd)
        >>> # result_spark, overall_spark = proc_freq(data_spark, tables=['A', 'B'], weights='W')
        >>> # result_spark[0].show() # Example action
    """

    if not isinstance(tables, list):
        raise TypeError("The 'tables' argument must be a list of column names.")

    if isinstance(data, pd.DataFrame):
        # Pandas DataFrame processing
        results_pd = []
        overall_weights_pd: Optional[Series] = None
        total_rows = len(data)
        if total_rows == 0:
             if not noprint: print("Warning: Input Pandas DataFrame is empty.")
             return [], None # Return empty results for empty dataframe


        if weights:
            if weights not in data.columns:
                raise ValueError(f"Weight column '{weights}' not found in DataFrame.")
            if data[weights].isnull().any():
                 raise ValueError(f"Weight column '{weights}' contains null values.")
            if not pd.api.types.is_numeric_dtype(data[weights]):
                 raise TypeError(f"Weight column '{weights}' must be numeric.")

            overall_weights_pd = data[weights].value_counts().sort_index()
            total_weight = data[weights].sum()
            if total_weight == 0:
                 print("Warning: Total weight is zero. Weighted percentages will be NaN or Inf.")


        for col in tables:
            if col not in data.columns:
                raise ValueError(f"Column '{col}' not found in DataFrame.")

            if weights:
                # Calculate weighted frequencies and percentages
                # Ensure the column used for grouping doesn't have NaNs temporarily for agg
                # Or handle NaNs explicitly if needed
                grouped_data = data.dropna(subset=[col]).groupby(col)[weights].agg(['count', 'sum'])
                result_df = grouped_data.reset_index().rename(
                    columns={'count': 'Frequency', 'sum': 'Weighted Frequency'}
                )
                # Calculate Percent based on total rows before dropping NaNs for grouping
                result_df['Percent'] = (result_df['Frequency'] / total_rows) * 100
                # Calculate Weighted Percent
                if total_weight > 0:
                     result_df['Weighted Percent'] = (
                         result_df['Weighted Frequency'] / total_weight
                     ) * 100
                else:
                     result_df['Weighted Percent'] = float('nan') # Assign NaN if total weight is zero

            else:
                # Calculate unweighted frequencies and percentages
                counts = data[col].value_counts(dropna=False).reset_index() # Include NaNs if present
                counts.columns = [col, 'Frequency']
                counts['Percent'] = (counts['Frequency'] / total_rows) * 100
                result_df = counts

            results_pd.append(result_df.sort_values(by=col).reset_index(drop=True)) # Sort results for consistency

        if not noprint:
            print(f"--- Frequency Analysis Results (Pandas) ---")
            for i, result in enumerate(results_pd):
                print(f"\nTable for column: '{tables[i]}'")
                print(result.to_string(index=False)) # Use to_string for better console output
            if weights and overall_weights_pd is not None:
                print("\nOverall Weights Distribution:")
                print(overall_weights_pd)
            print(f"-------------------------------------------")

        return results_pd, overall_weights_pd

    elif SparkDataFrame and isinstance(data, SparkDataFrame):
        # PySpark DataFrame processing
        results_spark = []
        overall_weights_spark: Optional["SparkDataFrame"] = None # type: ignore

        total_count = data.count()
        if total_count == 0:
            if not noprint: print("Warning: Input Spark DataFrame is empty.")
            return [], None # Return empty results for empty dataframe

        if weights:
            if weights not in data.columns:
                raise ValueError(f"Weight column '{weights}' not found in DataFrame.")
            # Add checks for nulls and numeric type in Spark
            weight_col_type = data.schema[weights].dataType
            # Simple check for numeric types in Spark (might need refinement for all types)
            if not isinstance(weight_col_type, (F.IntegerType, F.LongType, F.FloatType, F.DoubleType, F.DecimalType)):
                 raise TypeError(f"Weight column '{weights}' must be numeric in Spark DataFrame.")
            # Check for nulls (can be expensive on large data)
            # null_count = data.where(F.col(weights).isNull()).count()
            # if null_count > 0:
            #      raise ValueError(f"Weight column '{weights}' contains null values.")

            overall_weights_spark = (
                data.groupBy(weights).count().orderBy(weights)
            )
            total_weight_result = data.select(F.sum(weights).alias("total_w")).collect()
            total_weight = total_weight_result[0]['total_w'] if total_weight_result else 0
            if total_weight is None or total_weight == 0:
                 print("Warning: Total weight is zero or null. Weighted percentages will be NaN or Inf.")
                 total_weight = 0 # Avoid division by zero later


        for col in tables:
            if col not in data.columns:
                raise ValueError(f"Column '{col}' not found in DataFrame.")

            if weights:
                # Calculate weighted frequencies and percentages for Spark
                # Handle potential nulls in the grouping column if necessary
                weighted_counts = (
                    data.na.drop(subset=[col]) # Drop rows where the grouping column is null
                    .groupBy(col)
                    .agg(
                        F.count(col).alias("Frequency"),
                        F.sum(weights).alias("Weighted Frequency")
                      )
                )
                # Calculate Percent based on total count before dropping NaNs
                result_df = weighted_counts.withColumn(
                    "Percent", (F.col("Frequency") / total_count) * 100
                )
                # Calculate Weighted Percent
                if total_weight > 0:
                     result_df = result_df.withColumn(
                         "Weighted Percent", (F.col("Weighted Frequency") / total_weight) * 100
                     )
                else:
                     # Add a column with null or NaN if total_weight is zero
                     result_df = result_df.withColumn("Weighted Percent", F.lit(float('nan')))

                results_spark.append(result_df.orderBy(col)) # Sort results

            else:
                # Calculate unweighted frequencies and percentages for Spark
                # Include nulls in counts using groupBy(F.isnull(col)) or similar if needed
                # Standard groupBy treats nulls as a separate group
                counts = data.groupBy(col).count().withColumnRenamed("count", "Frequency")
                result_df = counts.withColumn(
                    "Percent", (F.col("Frequency") / total_count) * 100
                ).orderBy(col) # Sort results
                results_spark.append(result_df)

        if not noprint:
            print(f"--- Frequency Analysis Results (PySpark) ---")
            for i, result in enumerate(results_spark):
                print(f"\nTable for column: '{tables[i]}'")
                result.show()
            if weights and overall_weights_spark is not None:
                print("\nOverall Weights Distribution:")
                overall_weights_spark.show()
            print(f"--------------------------------------------")

        return results_spark, overall_weights_spark

    else:
        # Check if SparkDataFrame is None and raise appropriate error
        if SparkDataFrame is None and not isinstance(data, pd.DataFrame):
             raise TypeError(
                 "Input data is not a Pandas DataFrame. PySpark is not installed or available."
             )
        else:
             raise TypeError(
                 "Input data must be a Pandas DataFrame or a PySpark DataFrame."
             )