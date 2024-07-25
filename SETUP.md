# Setup
## macOS
### Homebrew
Homebrew is a package manager for macOS that makes it easy to install and manage software. Follow these steps to install Homebrew:

1. Open the Terminal.
2. Paste the following command and press Enter:
   ```shell
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    ```

### Installing Dependencies
Install anaconda
```shell
brew install --cask anaconda
```

Install jdk 11
```shell
brew install java11
```

Install Apache Spark
```shell
brew install apache-spark
```

Determine the Spark and Scala version
```shell
pyspark # The version will show at the top of the output
```
```pyspark
print(version) # This will print <your-spark-version>
print(".".join(spark.sparkContext._jvm.scala.util.Properties.versionNumberString().split(".")[:2])) # This will print <your-scala-version>
exit()
```

Configuring the Spark Project Application Logs (Log4J) - Be sure to change `<your-scala-version` and `<your-spark-version>` to the versions you found above
```shell
cd /opt/homebrew/Cellar/apache-spark/<your-spark-version>/libexec/conf
cp spark-defaults.conf.template spark-defaults.conf
echo "spark.driver.extraJavaOptions      -Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=hello-spark" >> spark-defaults.conf
echo "spark.jars.packages                org.apache.spark:spark-avro_<your-scala-version>:<your-spark-version>" >> spark-defaults.conf
```

Add Java and Spark to the PATH (bash_profile or zshrc)
```shell
# JDK
export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"
# Spark
export SPARK_HOME=/opt/homebrew/Cellar/apache-spark/<your-spark-version>/libexec
export PATH=$SPARK_HOME/bin:$PATH
```

If you are using conda, it will use the SPARK_HOME configured here. You will need to **restart PyCharm** in order for the
environment variables to take effect. Not restarting after setting these will result in things like logs not working as expected.

Set your python interpreter to a conda environment
1. PyCharm -> Settings... -> Project -> Python Interpreter
2. Add Interpreter -> Add Local Interpreter...
3. Select `Conda Environment` on the left panel -> `Create new environment` -> OK

Install pyspark
1. At the bottom left, there is an icon that looks like a stack of rectangles.
2. Click on it and search for "pyspark". 
3. Click on the `install` button, select the same version as `<your-spark-version>` from above and wait for it to finish.
