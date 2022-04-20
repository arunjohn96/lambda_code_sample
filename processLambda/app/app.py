import boto3
import os
import subprocess
import shlex
from boto3.dynamodb.conditions import Key
import requests
import json


SOURCE_BUCKET = os.environ['MEDIA_CAPTURE_BUCKET']
W_CALL_ACCESS_KEY = os.environ['W_CALL_ACCESS_KEY']
W_CALL_SECRET_KEY = os.environ['W_CALL_SECRET_KEY']
SOURCE_PREFIX = 'captures'
s3 = boto3.client('s3')
w_call_chime = boto3.client('chime', aws_access_key_id=W_CALL_ACCESS_KEY,
                            aws_secret_access_key=W_CALL_SECRET_KEY)
w_call_s3 = boto3.client('s3', aws_access_key_id=W_CALL_ACCESS_KEY,
                         aws_secret_access_key=W_CALL_SECRET_KEY)
dynamodb = boto3.resource('dynamodb', aws_access_key_id=W_CALL_ACCESS_KEY,
                          aws_secret_access_key=W_CALL_SECRET_KEY,
                          region_name='us-east-1')
MEETING_TABLE = os.environ['MEETINGS_TABLE_NAME']


def get_attendees(MEETING_ID, EXTERNAL_MEETING_ID):
    # table = dynamodb.Table(MEETING_TABLE)
    # attendees = table.query(
    #     IndexName='meetingIdIndex',
    #     KeyConditionExpression=Key('meetingId').eq(MEETING_ID))

    url = 'https://w-call-demo02.herokuapp.com//project/schedule/get_attendees_list'
    data = {
        "external_meeting_id": EXTERNAL_MEETING_ID,
        "meeting_id": MEETING_ID
    }
    headers = {
        'Content-Type': 'application/json'
    }
    req = requests.post(url, headers=headers, data=json.dumps(data))
    res = json.loads(req.text)
    print(res)
    attendeeIds = res.get('data', [])
    return attendeeIds


def process_files(objs_keys, MEETING_ID, EXTERNAL_MEETING_ID, MEDIA_PIPELINE, file_type, attendee=None):
    if attendee:
        attendeeStr = "-" + attendee[0]
    else:
        attendeeStr = ""

    with open('/mnt/efs/' + file_type + attendeeStr + '_list.txt', 'w') as f:
        for k in objs_keys:
            basename = os.path.splitext(k)[0]
            ffmpeg_cmd = "ffmpeg -i /mnt/efs/" + k + " -bsf:v h264_mp4toannexb -f mpegts -framerate 15 -c copy /mnt/efs/" + \
                basename + attendeeStr + "-" + file_type + ".ts -y"
            command1 = shlex.split(ffmpeg_cmd)
            p1 = subprocess.run(
                command1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            f.write(f'file \'/mnt/efs/{basename}{attendeeStr}-{file_type}.ts\'\n')

    ffmpeg_cmd = "ffmpeg -f concat -safe 0 -i /mnt/efs/" + file_type + attendeeStr + \
        "_list.txt  -c copy /mnt/efs/" + file_type + attendeeStr + ".mp4 -y"
    print(ffmpeg_cmd)
    command1 = shlex.split(ffmpeg_cmd)
    p1 = subprocess.run(command1, stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE)
    try:
        s3.upload_file('/mnt/efs/' + file_type + attendeeStr + '.mp4', SOURCE_BUCKET, "captures/" +
                       MEETING_ID + "/processed" + '/processed-' + file_type + attendeeStr + '.mp4')

    except Exception as e:
        print(e)

    try:
        url = 'https://w-call-demo02.herokuapp.com//project/recordings/get_path_for_write'
        data = {"external_meeting_id": EXTERNAL_MEETING_ID,
                "media_pipeline": MEDIA_PIPELINE}
        headers = {
            'Content-Type': 'application/json'
        }
        req = requests.post(url, headers=headers, data=json.dumps(data))
        res = json.loads(req.text)
        print(res)
        bucket_name = res.get('data').get('bucket_name')
        file_path = res.get('data').get('path')

        w_call_s3.upload_file('/mnt/efs/' + file_type + attendeeStr + '.mp4', bucket_name, file_path +
                              MEETING_ID + "/processed" + '/processed-' + file_type + attendeeStr + '.mp4')
    except Exception as e:
        print(e)
    processedUrl = s3.generate_presigned_url('get_object', Params={
                                             'Bucket': SOURCE_BUCKET, 'Key': "captures/" + MEETING_ID + "/processed" + '/processed' + attendeeStr + "-" + file_type + '.mp4'})

    return processedUrl


def emptyDir(folder):
    fileList = os.listdir(folder)
    print("Files present in directory::::", len(fileList))
    for f in fileList:
        filePath = folder + '/' + f
        if os.path.isfile(filePath):
            os.remove(filePath)
        elif os.path.isdir(filePath):
            newFileList = os.listdir(filePath)
            for f1 in newFileList:
                insideFilePath = filePath + '/' + f1
                if os.path.isfile(insideFilePath):
                    os.remove(insideFilePath)


def handler(event, context):
    # This demo is limited in scope to give a starting point for how to process
    # produced audio files and should include error checking and more robust logic
    # for production use. Large meetings and/or long duration may lead to incomplete
    # recordings in this demo.
    print(event)
    emptyDir('/mnt/efs')
    MEETING_ID = event.get('detail').get('meetingId')
    EXTERNAL_MEETING_ID = event.get('detail').get('externalMeetingId')
    MEDIA_PIPELINE = event.get('detail').get('mediaPipelineId')
    print(MEETING_ID, EXTERNAL_MEETING_ID, MEDIA_PIPELINE)

    audioPrefix = SOURCE_PREFIX + '/' + MEETING_ID + '/audio'
    videoPrefix = SOURCE_PREFIX + '/' + MEETING_ID + '/video'

    audioList = s3.list_objects_v2(
        Bucket=SOURCE_BUCKET,
        Delimiter='string',
        MaxKeys=1000,
        Prefix=audioPrefix
    )
    audioObjects = audioList.get('Contents', [])
    print(audioObjects)

    videoList = s3.list_objects_v2(
        Bucket=SOURCE_BUCKET,
        Delimiter='string',
        MaxKeys=1000,
        Prefix=videoPrefix
    )
    videoObjects = videoList.get('Contents', [])
    print(videoObjects)

    if videoObjects:
        file_list = []
        file_type = 'video'
        for object in videoObjects:
            path, filename = os.path.split(object['Key'])
            # s3.download_file(SOURCE_BUCKET, object['Key'], '/mnt/efs/' + filename)
            file_list.append(filename)

        vid_keys = list(filter(lambda x: 'mp4' in x, file_list))
        print(vid_keys)
        attendees = get_attendees(MEETING_ID, EXTERNAL_MEETING_ID)
        for attendee in attendees:
            print("Concatenating " + file_type +
                  " files for " + attendee + "...")
            attendeeVidKeys = list(filter(lambda x: attendee in x, vid_keys))
            print(attendeeVidKeys)
            download_files = []
            for object in videoObjects:
                path, filename = os.path.split(object['Key'])
                if filename in attendeeVidKeys:
                    s3.download_file(
                        SOURCE_BUCKET, object['Key'], '/mnt/efs/' + filename)
                    download_files.append(filename)
            print("Downloaded Files for {}".format(attendee))
            print(download_files)
            if download_files:
                process_files(attendeeVidKeys, MEETING_ID, EXTERNAL_MEETING_ID,
                              MEDIA_PIPELINE, file_type, [attendee])
                emptyDir('/mnt/efs')

    else:
        print("No videos")

    file_list = []
    file_type = 'audio'
    for object in audioObjects:
        path, filename = os.path.split(object['Key'])
        s3.download_file(SOURCE_BUCKET, object['Key'], '/mnt/efs/' + filename)
        file_list.append(filename)

    objs_keys = filter(lambda x: 'mp4' in x, file_list)
    process_files(objs_keys, MEETING_ID, EXTERNAL_MEETING_ID,
                  MEDIA_PIPELINE, file_type)

    emptyDir('/mnt/efs')
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST'
        }
    }
