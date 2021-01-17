#%%
import setupspark
setupspark.init()
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

#%%
# if not already created automatically, instantiate Sparkcontext
spark = SparkSession.builder \
    .appName("BigData") \
    .master("spark://127.0.0.1:7077") \
    .config("spark.local.dir", 'spark_temp') \
    .config("spark.worker.cleanup.enabled", 'true') \
    .config("spark.sql.execution.arrow.enabled", 'true') \
    .config('spark.executor.cores', '8') \
    .config('spark.executor.instances', '1') \
    .config('spark.executor.memory', '4g') \
    .config('spark.dynamicAllocation.enabled', 'false') \
    .config('spark.dynamicAllocation.maxExecutors', '1') \
    .getOrCreate()

spark

#%%
def read_and_split(sp_df, test_size):
    from pyspark.ml.feature import VectorAssembler
    assembler = VectorAssembler(inputCols = sp_df.columns[:-1], outputCol="features")
    features = assembler.transform(sp_df)

    # Split the data into train and test
    splits = features.randomSplit([1 - test_size, test_size])
    train = splits[0]
    train.count()   # force loading of data
    test = splits[1]
    test.count()    # force loading of data

    num_classes = sp_df.select("defect_type").distinct().count()
    labelCol = sp_df.columns[-1]

    return train, test, len(sp_df.columns), num_classes, labelCol

def train_evaluate(train, test, hidden_layers, num_columns, num_classes, labelCol):
    # specify layers for the neural network:
    layers = [num_columns - 1, *hidden_layers, max(2, num_classes)]

    # create the trainer and set its parameters
    from pyspark.ml.classification import MultilayerPerceptronClassifier
    trainer = MultilayerPerceptronClassifier(labelCol=labelCol, maxIter=500, layers=layers)

    # train the model
    model = trainer.fit(train)

    # compute accuracy on the test set
    result = model.transform(test)
    predictionAndLabels = result.select("prediction", labelCol)

    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    evaluator = MulticlassClassificationEvaluator(labelCol=labelCol, metricName="accuracy")
    return evaluator.evaluate(predictionAndLabels)

#%%
# load, arange and split the data
test_size = 0.2
hidden_layers = (50,100,50)
sp_df: DataFrame = spark.read.load(r"data\parquets")
# sp_df = sp_df.sample(0.000128)    # take only a sample of the data
train, test, num_columns, num_classes, labelCol = read_and_split(sp_df, test_size)

# some statistics
print("Train data count:", train.count())
train.groupBy("defect_type").count().show()
print("Test data count:", test.count())
test.groupBy("defect_type").count().show()

#%%
train_evaluate(train, test, hidden_layers, num_columns, num_classes, labelCol)