# establishing environment 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from acquire import get_311_data

spark = SparkSession.builder.getOrCreate()

def wrangle_311_data():
    """
    No arguments needed. Returns 311 data combined and prepped for exploration.
    """
    # using acquire file function to get data
    source, case, dept = get_311_data()

    # converting case_closed and case_late from strings to booleans
    # this will allow us to perform boolean operations on these columns should we need to in the future
    case = case.withColumn("case_closed", expr('case_closed == "YES"')).withColumn("case_late", expr('case_late == "YES"'))

    # converting council_district to string since we won't be performing mathematical operations on it
    case = case.withColumn("council_district", col("council_district").cast("string"))

    # setting date and time format
    fmt = "M/d/yy H:mm"

    # converting dates to date time format
    case = (
    case.withColumn("case_opened_date", to_timestamp("case_opened_date", fmt))
    .withColumn("case_closed_date", to_timestamp("case_closed_date", fmt))
    .withColumn("case_due_date", to_timestamp("case_due_date", fmt))
    )

    # combining DFs
    case = (
    case
    # left join on dept_division
    .join(dept, "dept_division", "left")
    # drop all the columns except for standardized name, as it has much fewer unique values
    .drop(dept.dept_division)
    .drop(dept.dept_name)
    .drop(case.dept_division)
    .withColumnRenamed("standardized_dept_name", "department")
    # convert to a boolean
    .withColumn("dept_subject_to_SLA", col("dept_subject_to_SLA") == "YES")
    )

    # left join on source_id 
    case = (case.join(source, "source_id", "left"))

    # returning DF
    return case

