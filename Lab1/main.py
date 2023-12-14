from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import pandas as pd

def main():
    spark = SparkSession.builder.appName("FilterJson").getOrCreate()

    json_file_path = '10K.github.json'
    df = spark.read.json(json_file_path)
    filtered_df = df.filter((col("type") == "PushEvent") & (expr("size(payload.commits) > 0")))
    filtered_df = filtered_df.filter(
        expr("exists(payload.commits, commit -> size(split(commit.message, ' ')) == 3)")
    )

    filtered_data = filtered_df.select("actor.display_login", "payload.commits.message").rdd.collect()
    df_pandas = pd.DataFrame(filtered_data, columns=["Author", "Message"])
    csv_output_path = 'output.csv'
    df_pandas.to_csv(csv_output_path, index=False)

    spark.stop()
    pass

if __name__ == '__main__':
    main()
