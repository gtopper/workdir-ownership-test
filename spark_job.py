import mlrun
from mlrun.projects import MlrunProject
from pyspark.sql import SparkSession

# not really using the project, but still checking if it can be loaded
project: MlrunProject = mlrun.get_or_create_project(
    name="pyspark-iris2", context="./", user_project=True
)
read_obj = SparkSession.builder.getOrCreate().read
url = "v3io://bigdata/example_iris_data.csv"
print(f"{url=}")
df = read_obj.load(url, format="csv")
print(f"{df=}")
print(f"{df.summary()=}")

