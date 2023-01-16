from pyspark.sql.functions import *

class DataValidator():
    """
    Check whether a table has at least one observation or not
    Also checks if all the table have at least one observation or not
    """
    
    def __init__(self, spark, output, path):
        self.spark = spark
        self.output = output
        self.path = path
        
    def check_immigration(self):
        """
        Check the number of rows in the immigration table
        Returns:
            True if it has at least one or more than one observation or False if it has no observation
        """
        immigration_spark = self.spark.read.parquet("{}{}".format(self.output, self.path["countries"]))
        rows = immigration_spark.count()
        return rows > 0
    
    def check_state(self):
        """
        Check the number of rows in the stae table
        Returns:
            True if it has at least one or more than one observation or False if it has no observation
        """
        state_spark = self.spark.read.parquet("{}{}".format(self.output, self.path["state"]))
        rows = state_spark.count()
        return rows > 0
    
    def check_city(self):
        """
        Check the number of rows in the city table
        Returns:
            True if it has at least one or more than one observation or False if it has no observation
        """
        city_spark = self.spark.read.parquet("{}{}".format(self.output, self.path["city"]))
        rows = city_spark.count()
        return rows > 0
    
    def check_temperature(self):
        """
        Check the number of rows in the temperature table
        Returns:
            True if it has at least one or more than one observation or False if it has no observation
        """
        temperature_spark = self.spark.read.parquet("{}{}".format(self.output, self.path["temperature"]))
        rows = temperature_spark.count()
        return rows > 0
    
    def check_demographic(self):
        """
        Check the number of rows in the demographic table
        Returns:
            True if it has at least one or more than one observation or False if it has no observation
        """
        demographic_spark = self.spark.read.parquet("{}{}".format(self.output, self.path["demographic"]))
        rows = demographic_spark.count()
        return rows > 0
    
    def check_visa(self):
        """
        Check the number of rows in the visa table
        Returns:
            True if it has at least one or more than one observation or False if it has no observation
        """
        visa_spark = self.spark.read.parquet("{}{}".format(self.output, self.path["visa"]))
        rows = visa_spark.count()
        return rows > 0
    
    def check_countries(self):
        """
        Check the number of rows in the countries table
        Returns:
            True if it has at least one or more than one observation or False if it has no observation
        """
        countries_spark = self.spark.read.parquet("{}{}".format(self.output, self.path["countries"]))
        rows = countries_spark.count()
        return rows > 0
    
    def check_mode(self):
        """
        Check the number of rows in the mode table
        Returns:
            True if it has at least one or more than one observation or False if it has no observation
        """
        mode_spark = self.spark.read.parquet("{}{}".format(self.output, self.path["mode"]))
        rows = mode_spark.count()
        return rows > 0
    
    def check_all(self):
        """
        Check if all the tables have at least one observation or not
        Returns:
            True if all the tables have at least one or more than one observation or False if it has no observation
        """
        full_result = self.check_immigration() and self.check_state() and self.check_city() and self.check_temperature() and self.check_demographic() and self.check_visa() and self.check_countries() and self.check_mode()
        return full_result
    
    
    def left_join_two_tables(self, table1, table2, common_field1, common_field2):
        """
        Join two PySpark dataframes or tables using left join and using the common fields specified for the two tables
        
        Arguments:
            table1: A table where we want to join fields from other table
            table2: A second table columns of which we want to copy to table1
            common_field1: A common field in table1 using which the join will be done
            common_field2: A common field in table2 using which the join will be done
            
        Returns:
            returns the left joined table containing all columns of table1 and table2
        """
        
        self.table1 = table1
        self.table2 = table2
        self.common_field1 = common_field1
        self.common_field2 = common_field2
        new_table = self.table1.join(self.table2, self.table1[self.common_field1] == self.table2[self.common_field2], "left")
        return new_table
    
    
    def validate_left_join(self, table1, table2, common_field1, common_field2):
        result = self.left_join_two_tables(table1, table2, common_field1, common_field2)
        rows = result.count()
        return rows > 0