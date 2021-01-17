from pathlib import Path

def init(spark_home: Path = None):
    import sys, os, glob

    if spark_home is None:
        spark_home = Path("spark")
    spark_python = str(spark_home / "python")

    os.environ["HADOOP_HOME"] = str(spark_home)
    os.environ["PATH"] = str(spark_home / "bin") + ";" + os.environ["PATH"]

    py4j = glob.glob(os.path.join(spark_python, 'lib', 'py4j-*.zip'))[0]
    sys.path[:0] = [spark_python, py4j]