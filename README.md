# Spark-Batch-processing-on-AWS
To perform Spark Transformations on bank transactions using a real-time currency ticker API and loading the processed data to Athena using Glue Crawler.

API open exchange rate API - To get the Forex Exchange rates 


# AWS instance and connecting to it and then installing docker into it 

## Set us SSH connection with EC2 instance 

#### Requirements
* SSH Key (.pem file) provided by Amazon. This SSH key is provided by Amazon when you launch the instance.
* IP address. IP address assigned to your EC2 instance.
* Username. The username depends on the Linux distro you just launched. Usually, these are the usernames for the most common distributions: Example -if instance is Ubuntu: ubuntu <-- Unsername

#### Steps 
* Connect to ec2 instance ssh using linux 
1. Open your terminal and change directory with command cd, where you downloaded your pem file. In this demonstration, pem file is stored in the downloads folder.
   
2. Type the SSH command with this structure:

```
ssh -i file.pem username@ip-address
```
The explanation of the command:

username: Username that uses your instance
ip-address: IP address given to your instance

3. After pressing enter, a question will prompt to add the host to your known_hosts file. Type **YES** .
This will help to recognize the host each time you’re trying to connect to your instance.

4. And that’s it! Now you’re logged in on your AWS instance

**For more info visit this page** :- https://www.clickittech.com/aws/connect-ec2-instance-using-ssh/ 

## Installing docker into our EC2 instance 

```
#Update the packages on your instance
sudo yum update -y
```

```
#Install Docker
sudo yum install docker -y
```

```
#Start the Docker Service
sudo service docker start
```

```
<!-- Add the ec2-user to the docker group so you can execute Docker commands without using sudo. -->
sudo usermod -a -G docker ec2-user
```
**After this step just relogin into your EC2 instance and You should be able to run the docker commands**

## Creating AWS EMR instance
* **Note** While Creating the EMR instance make sure to select the key pair of the EC2 instance and also check for the IAM Role for it.

Now our Ec2 user doesnth have access to the cluster directly. To access the hadoop env and spark in our EMR cluster 

* Write the following code to connect with the EMR cluster using SSH connection

```
ssh -i file.pem username@'Master public DNS of your EMR istanmce'
```

Then write to get into our HDFS cluster and when you write **pyspark** so you can start and access the spark 

```
sudo su hadoop
```

## Adding Our Docker file into the EC2 - docker setup we established earlier 

Link:- https://docs.aws.amazon.com/managedservices/latest/appguide/qs-file-transfer.html

### After adding the files run the following commands 
```
docker build -t mudra . -f Dockerfile     
```
```
docker run -dit mudra                     
```
```
docker exec -it 46b6bd22cf86 /bin/bash 
```


























