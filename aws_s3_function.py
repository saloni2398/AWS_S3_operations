import logging
import boto3
from botocore.exceptions import ClientError
import botocore


def upload_file(file_name, bucket, object_name=None):
    
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return 0
    return 1


#delete  bucket
def delete_bucket(bucket_name):


    # Delete the bucket
    s3 = boto3.client('s3')
    try:
        s3.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return 0
    return 1


#delete objects
def delete_objects(bucket_name, object_names):


    # Convert list of object names to appropriate data format
    objlist = [{'Key': obj} for obj in object_names]

    # Delete the objects
    s3 = boto3.client('s3')
    try:
        s3.delete_objects(Bucket=bucket_name, Delete={'Objects': objlist})
    except ClientError as e:
        logging.error(e)
        return 0
    return 1


#list of objects
def list_bucket_objects(bucket_name):
    

    # Retrieve the list of bucket objects
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
    except ClientError as e:
        # AllAccessDisabled error == bucket not found
        logging.error(e)
        return None
    return response['Contents']


#create bucket
def create_bucket(bucket_name, region=None):

    # Create bucket
    s3_client = boto3.client('s3')
    try:
        if region is None:
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return 0
    return 1



#delete single object

def delete_object(bucket_name, object_name):
    # Delete the object
    s3 = boto3.client('s3')
    try:
        s3.delete_object(Bucket=bucket_name, Key=object_name)
    except ClientError as e:
        logging.error(e)
        return 0
    return 1
#download

def download(bucket_name,key,outname):
    Bucket = bucket_name
    Key = key
    outPutName = outname

    s3 = boto3.resource('s3')
    try:
        s3.Bucket(Bucket).download_file(Key, outPutName)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

#main fuction
def main():
    print("enter the choice of operation:")
    print("1.create bucket")
    print("2.delete bucket")
    print("3.delete single object")
    print("4.delete all objects")
    print("5.upload object")
    print("6.download object")

    ch = input("Enter your value: ")
    if ch=='1':
        n = input("Enter bucket name: ")
        choice = input("want to give region name y/n??")
        if choice == 'y':
            r = input("Enter region: ")
            print("bucket created!", create_bucket(n, r))
        else:
            create_bucket(n)
            print("bucket created!")

    elif ch=='2':
        n = input("Enter bucket name: ")
        status = delete_bucket(n)
        if status == 0:
            print("Bucket not empty please delete all objects first!")
        else:
            print("Bucket deleted successfully")
    elif ch=='3':
        n = input("Enter bucket name: ")
        o = input("Enter object name: ")
        print("object deleted",delete_object(n,o))
    elif ch=='4':
        lst = []
        n = input("Enter bucket name: ")

        for key in list_bucket_objects(n):
            ele = key['Key']
            lst.append(ele)  # adding the element

        for i in lst:
            delete_objects(n, lst)
        print("all object deleted")

    elif ch=='5':
        f = input("Enter file_name: ")
        n = input("Enter bucket name: ")
        choice = input("want to give optional name y/n??")
        if choice=='y':
            o = input("Enter object name: ")
            print("success", upload_file(f, n,o))
        else:
            print("success", upload_file(f, n))
    elif ch=='6':
        f = input("Enter file_name: ")
        n = input("Enter bucket name: ")
        o = input("Enter optional name: ")
        download(n,f,o)
        print("download completed!")

    else:
        print("enter correct value!")


if __name__ == "__main__":
	main()


