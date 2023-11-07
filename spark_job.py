import mlrun
from mlrun.projects import MlrunProject
from pyspark.sql import SparkSession

# not really using the project, but still checking if it can be loaded
project: MlrunProject = mlrun.get_or_create_project(
    name="pyspark-iris", context="./", user_project=True
)
read_obj = SparkSession.builder.getOrCreate().read
# weirdly, v3io:///projects/ is automatically added to the head of the url
url = "pyspark-iris-alp-aribal/artifacts/example_iris_data.csv"
print(f"{url=}")
df = read_obj.load(url, format="csv")
print(f"{df=}")
print(f"{df.summary()=}")

