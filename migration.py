#main program for github
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, lit, when


class DataTransfer():
    #it will read the config.csv and we will extract the path to input table, its pk columns and path to target table
    def read_csv(self, csv_path):
        config_df = spark.read.format("csv").option("header", True).load(csv_path)
        self.pk_cols = config_df.filter(col("key") == "pk_cols").select("value").collect()[0]["value"].split(",")
        self.input_path = config_df.filter(col("key") == "input_path").select("value").collect()[0]["value"]
        self.target_path = config_df.filter(col("key") == "target_table").select("value").collect()[0]["value"]
        
    #This funciton will be used to migrate data from input table to target table with given conditions        
    def migrate_data(self, csv_path):
        self.read_csv(csv_path)
        
        #reading input and target df
        input_df = spark.read.format("csv").option("header", True).load(self.input_path)
        try:
            target_df = spark.read.format("csv").option("header", True).load(self.target_path)
        except:
            #create empty dataframe if no dataframe present at target path
            df_schema = input_df.schema
            new_column = StructField("IUD", StringType(), True)
            new_schema = StructType(df_schema.fields + [new_column])
            target_df = spark.createDataFrame([], schema=new_schema)

        # u_df consist of all the common records that are present in both the input and output tables with updated values
        u_df = input_df.join(target_df.select(*self.pk_cols), on = self.pk_cols, how = "inner")
        
        # j_df consist of all the common records that are present in both the input and output tables with old values
        j_df = target_df.join(input_df.select(*self.pk_cols), on = self.pk_cols, how = "inner")
        
        # we drop IUD column from j_df
        ju_df = j_df.drop("IUD")
        
        #udf consist all the values that are present in u_df but not in ju_df i.e all the records that are updated
        udf = u_df.subtract(ju_df)
        
        #idf consist of all the values that are present in u_df but not in udf i.e. all the records that has no change
        idf = u_df.subtract(udf)
        
        # it_df consist of all the records that are not present in target table but present in input table so will be inserted
        it_df = input_df.join(target_df.select(*self.pk_cols), on = self.pk_cols, how = "anti")
        
        # ddf consist of all the records that are present in target table but not in input table so will be marked deleted
        ddf = target_df.join(input_df.select(*self.pk_cols), on = self.pk_cols, how='anti')
        
        # marking udf records with U
        udf = udf.withColumn("IUD", lit("U"))
        
        # marking idf records with I
        idf = idf.withColumn("IUD", lit("I"))
        
        # marking it_df records with I
        it_df = it_df.withColumn("IUD", lit("I"))
        
        # marking ddf recrds with D
        ddf = ddf.withColumn("IUD", lit("D"))
        
        # unioning all the dataframes 
        target_df = idf.union(it_df).union(udf).union(ddf)
        
        #saving the dataframe into target path
        target_df.write.mode('overwrite').format('csv').save(self.target_path)
        
        
transfer = DataTransfer()
transfer.migrate_data("config.csv")