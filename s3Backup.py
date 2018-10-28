import os
import boto3
from boto3 import Session
import time
from dateutil import parser
import pytz
from six.moves import input

def newSession():
    return Session(aws_access_key_id = input("Enter your AWS access key ID: "),
                   aws_secret_access_key = input("Enter your AWS secret key: "),
                   region_name = "us-east-2")

def recFiles(bucketName, full_path, old_dir):
    for root, dirs, files in os.walk(full_path):
        current_path = root + "\\"
        current_dir = old_dir + os.path.basename(os.path.normpath(current_path))

        for next_dir in dirs:
            parse = old_dir + "/" + os.path.basename(current_dir)
            recFiles(bucketName, current_path + next_dir, parse.replace("//", "/") + "/")

        if not files:
            print("Uploading: " + current_dir)
            s3.Object(bucketName, current_dir + "/").put(Body="")

        for file in files:
            s3Key = current_dir + "/" + file
            objs = list(bucket.objects.filter(Prefix = s3Key))
            if len(objs) > 0 and objs[0].key == s3Key:
                objTime = s3.Object(bucketName, current_dir + "/" + file).last_modified
                dt = parser.parse(time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(os.path.getmtime(current_path + "\\" + file))))
                nObjTime = objTime.replace(tzinfo=None)
                if nObjTime < dt:
                    print("Uploading: " + current_dir + "/" + file)
                    s3.Object(bucketName, current_dir + "/" + file).put(Body = open(current_path + file, "rb"))
            else:
                print("Uploading: " + current_dir + "/" + file)
                s3.Object(bucketName, current_dir + "/" + file).put(Body = open(current_path + file, "rb"))
        return None
    return None

print("Welcome to s3Backup tool! Please follow the prompt below:")
inSession = input("Are user credentials already set up? If not, you will be prompted to set up AWS access ID & secret key (y/n):")
if inSession == "n" or inSession == "N" or inSession == "No" or inSession == "no":
    nSession = newSession()
    s3 = nSession.resource("s3")
    client = nSession.client("s3")
else:
    s3 = boto3.resource("s3")
    client = boto3.client("s3")

for i in range(0,10):
    try:
        bucketName = input("Please enter the name of the bucket: ")
        s3.create_bucket(Bucket=bucketName, CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
    except client.exceptions.BucketAlreadyOwnedByYou:
        break
    except client.exceptions.BucketAlreadyExists:
        print("Bucket " + bucketName + " is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.")
        continue
else:
    print("You have reached the maximum failed attempts. Please restart the program to try again.")

bucket = s3.Bucket(bucketName)
path = input("Please enter the directory path to backup: ")
print("Uploading process started...")
recFiles(bucketName, path, "Backup/")
print("Uploading done!")
exit = input("Press ENTER to exit.")