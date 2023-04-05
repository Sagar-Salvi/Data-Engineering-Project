# <h1 align="center">Data-Engineering-Project</h1>
# ![image](https://user-images.githubusercontent.com/109748639/229044146-880c88f3-ef27-44b5-b3ab-88bd4cd62b33.png)

----------------------------------------------------------------------------------------------------------------------------------------------------------------

# Introduction

The Centralized Data Warehouse and ML Solution for Banking Analytics is a project that combines a centralized repository for banking data with machine learning algorithms to enable predictive analysis.

-------------------------------------------------------------------------------------------------------------------------------------------------------------

# Problem Statement

Our client, a bank, has various data sources, that contain valuable data. However, the data is not well-organized, making it challenging to analyze and gain insights. Additionally, the client wants to use the data to create machine learning models to improve their business processes and provide better services to their customers.

------------------------------------------------------------------------------------------------------------------------------------------------------------

# Services And Tools

AWS Cloud Services :- EC2 Instance, IAM, S3 Bucket, RDS Database, Redshift Cluster, SNS, Sagemaker

Containerization-Orchestration Tool :- Docker, Kubernetes

Visualization Tools :- Tableau or Power Bi

MLOps Tool :- Kubeflow

Programming Language :- Python

Machine Learning - Scikit Learn

Apache spark

Apache Airflow

Jupyter Notebook

-------------------------------------------------------------------------------------------------------------------------------------------------------------

# Architecture Diagram

![Project](https://user-images.githubusercontent.com/109748639/229056317-5b6175e8-9686-4d7b-80b7-fc23af280a35.jpg)

-----------------------------------------------------------------------------------------------------------------------------------------------------------------

# First We'll create Our Client Data Sources

###  1] Create one AWS S3 bucket

Enter bucket name -> Select your Region

In Object ownership -> select ACLs Enabled

Enable all public access

Enable bucket Version

Create Bucket
        
   (Note:- In this bucket add 4000+ records csv file)
 
---------------------------------------------------------------------------------------------------------------------------------------------------------------------
   
### 2] Now Create second data source RDS MySQL Database

Search RDS on Search bar -> Select Database -> Create Database
    
Choose Standard Create method -> Select MySQL as Engine Type -> In Templates Select Free tier

In Setting First Enter Database Name -> Enter Username -> Enter Your Password

In Instance Select t2.micro or any other Instance type as per your requrirement

In Storage select gp2 type and 20GB or More storage as per your requrirement -> Disable Storage autoscaling

In Connectivity -> Select Don't Connect to EC2 and Network Type IPv4 select Default VPC or your Created VPC and Subnet ->
Select Yes in Public access -> Select your Created Security Group -> In additional Configuartion keep 3306 port number
    
select Password Authentication in Database authentication

In Additional Configuration -> Enter Database name -> Disable automated backups 
Other option keep as it is

Click Create Database


Now Connect this database to Local MySQL Workbench to insert client Data.

   (Note:- Here in this add 500+ records csv file)
   
----------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Now Our Client Data sources are created.

So now let's create our AWS Storage and tool setup.


First We'll create two AWS S3 bucket one for Staging Area and Second for ML Team

So Just follow this step to create bucket

## Bucket one

Enter bucket name-> staging_area -> Select your Region

In Object ownership -> select ACLs Enabled

Enable all public access

Enable bucket Version

Create Bucket
    
## Bucket one 

Enter bucket name-> ml_team -> Select your Region

In Object ownership -> select ACLs Enabled

Enable all public access

Enable bucket Version

Create Bucket


Both buckets are created. 

---------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Here we have to create one Security Group
    
In Name -> Enter SG name

In Description -> Enter Specific Description 
   
In vpc -> select default vpc

In Inbound Rule Add some extra rule -> 1] All traffic and Source Anywhere IPv4

2] Redshift and Source Anywhere IPv4

3] MySQL and Source Anywhere IPv4

Create Security Group

Now Create One IAM User User With Full AdministratorAccess policy
    
Download user's Access and Secret Key 

--------------------------------------------------------------------------------------------------------------------------------------------------------------

# Now Let's Create Our AWS Redshift Centralized Data Warehouse

Go to AWS Redshift -> Click On Cluster
 
In identifire -> Enter Cluster name -> select dc2.large Node Type -> Select 1 Number of Node
   
In Database configuration -> Enter Username -> Enter Your Password
   
In Associated IAM Roles -> Select Created One IAM Role(Give Full Admin Access for Demo Purpose)
 
Create Cluster 

It will take upto 10-12 min to avilable after that we have to Edit Configuration and select our created Security
Group instead of Default SG. Also select Turn On Publicly Accessible.

-----------------------------------------------------------------------------------------------------------------------------

# After this we are going to install Apache Airflow On EC2 Instance

(Note:- While creating an instance add one inbound rule in the security group - add Custom TCP rule and enter port as 8080)


  Step 1:
  
    sudo apt update 
  
    sudo apt upgrade -y


  Step 2:

    sudo apt install python3-pip


  Step 3:

    sudo apt install sqlite3


  Step 4:

    sudo apt install python3.10-venv  (If it gives an error then run this - sudo apt install python3.8-venv )


  Step 5:

    python3 -m venv venv

    source venv/bin/activate


  Step 6:

    i] sudo apt-get install libpq-dev

    ii] pip install "apache-airflow[postgres]==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.7.txt"  

(If It Gives an Error then run this - pip install "apache-airflow[postgres]==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.10.txt" )

    iii] airflow db init


  Step 7:

    i] sudo apt-get install postgresql postgresql-contrib

    ii] sudo -i -u postgres

    iii] psql

	    1] CREATE DATABASE airflow;
	    2] CREATE USER airflow WITH PASSWORD 'airflow';
      3] GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

Hit Ctrl+D wait and one more time Ctrl+D


  Step 8:

    i] cd airflow/

    ii] sed -i 's#sqlite:////home/ubuntu/airflow/airflow.db#postgresql+psycopg2://airflow:airflow@localhost/airflow#g' airflow.cfg

    iii] sed -i 's#SequentialExecutor#LocalExecutor#g' airflow.cfg


  Step 9:

    i] airflow db init

    ii] airflow users create -u airflow -f airflow -l airflow -r Admin -e example@gmail.com


  Step 10:

    i] airflow webserver &

    ii] airflow scheduler  (Run this command on the duplicate session)


## Now Copy public DNS

    eg:   ec2-18-220-39-100.us-east-2.compute.amazonaws.com:8080 (replace this dns with your instance DNS and add port 8080)

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Just Like this now we are going to install Apache Spark with Jupyter Notebook

So Just follow this installation steps.
    
  Step 1:

  Select AWS Ubuntu OS with t2.medium instance type and creating instance add IAM FullAdministrativeAccess Role to this instance
  (Note:- For Demo Purpose we are giving Full Administrator Access Role)

  Step 2:

  Update the packages

    sudo apt-get update
    sudo apt-get upgrade


  Step 3:

  Now start installing pip and java

1] Download pip

    sudo apt install python3-pip
	
Check pip version
	
    pip3 --version

2] Download Java

    pip3 install py4j

    sudo apt-get install openjdk-8-jdk


  Step 4:

Now Install Jupyter Notebook

    sudo apt install jupyter-notebook


  Step 5: 

Now We have to Configuring Jupyter Notebook settings

    jupyter notebook --generate-config

After this commnad one .py file is created. So we have to configure it.

    cd .jupyter

    nano jupyter_notebook_config.py

    ls

Now in this file we have to Increase the "#c.NotebookApp.iopub_data_rate_limit" value to "100000000" and remove the # from the front.

    c.NotebookApp.iopub_data_rate_limit = 100000000

    cd

  Step 6:

Now install Apache Spark

    pip3 install findspark

    wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz


Unzip it and move it to your /opt folder and create a symbolic link 

    tar xvf spark-*

    sudo mv spark-3.3.2-bin-hadoop3 /opt/spark-3.3.2

    sudo ln -s /opt/spark-3.3.2 /opt/spark


Now Configure Spark & Java & PySpark driver to use Jupyter Notebook

    nano ~/.bashrc

Now Enter following content here

    export SPARK_HOME=/opt/spark
    export PATH=$SPARK_HOME/bin:$PATH
    export JAVA_HOME='/usr/lib/jvm/java-8-openjdk-amd64'
    export PATH=$JAVA_HOME/bin:$PATH
    export PYSPARK_PYTHON=python3
    export PYSPARK_DRIVER_PYTHON=jupyter
    export PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip 0.0.0.0 --port=8888'


After saving path, you need to run

    source ~/.bashrc


  Step 7:

Connecting the Jupyter Notebook from your Web-Browser

First Enter following command

    pyspark


(Note:- As you can see, there is a URL given in the last line. Copy the contents of the URL after token=, i.e. c6f2835c731caf65c9413a866113c015ae2a589c0a9abc31 in my case.)

Now open Browser

    https://<your public dns>:8888


After opening Paste the copied token and create a new password if you want


  Step 8:

For connecting to jars we have to download some jar files in it

    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.183/aws-java-sdk-1.12.183.jar
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.12.183/aws-java-sdk-core-1.12.183.jar
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.0.0/hadoop-aws-3.0.0.jar
    wget https://repo1.maven.org/maven2/com/databricks/spark-redshift_2.11/3.0.0-preview1/spark-redshift_2.11-3.0.0-preview1.jar
    wget https://repo1.maven.org/maven2/io/github/spark-redshift-community/spark-redshift_2.12/5.1.0/spark-redshift_2.12-5.1.0.jar
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.183/aws-java-sdk-s3-1.12.183.jar

Download all this jar file into /opt/spark/jars dictionary

here also paste redshift cluster jar file to make connection between spark and redshift cluster (You can download this from your Redshift Created Cluster)


Step 9:

To stop jupyter notebook services

    jupyter notebook stop 8888  
    
-----------------------------------------------------------------------------------------------------------------------------------------------------

# Now we are going to install Kubeflow For Machine Learning Model Deployement

So Just follow this installation steps.



-------------------------------------------------------------------------------------------------------------------------------------------------------

# Our Workflow is ready. Now We can start our Work

task 1

Create Data Extraction Pipeline Using Apache Spark to extract client side data into our staging area bucket.

task 2

Read data from S3 staging area bucket using Apache Spark and Load it in our Redshift Data warehouse. Perform ETL Process here.

task 3

On Apache Spark new notebook read data from Redshift data warehouse and load some data in ml-team bucket to build Machine Learning model

task 4

Connect any Visualzation tool like Power Bi or tableau to Redshift cluster to Analyze the data and gain insights.

task 5

Read Data from ml-team s3 bucket to build ML model using Kubeflow.

-------------------------------------------------------------------------------------------------------------------------------------------------------

# Screenshot

## Apache Airflow Dags(Data Extraction Pipeline)

![Screenshot_1](https://user-images.githubusercontent.com/109748639/229997344-83316377-15c3-4af3-a670-2795adf16f96.png)


-----------------------------------------------------------------------------------------------------------------------------------------------------------
