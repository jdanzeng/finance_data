import json
import boto3
import os
import subprocess
import sys
import base64
base64.b64encode(b'\n')

subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", "/tmp", 'yfinance'])
sys.path.append('/tmp')
import yfinance as yf

def lambda_handler(event, context):
    stocks = ['FB','SHOP','BYND','NFLX','PINS','SQ','TTD','OKTA','SNAP','DDOG']
    data = yf.Tickers(stocks)
    hist = data.history(start="2020-05-14", end="2020-05-15", interval="1m", group_by = 'tickers')
    output = []

    for stock in stocks:
        for index, price in hist[stock].iterrows():
            output.append({'high':price['High'],'low':price['Low'],
            'ts':index.strftime('%Y-%m-%d %H:%M:%S'),'name':stock +"Cg=="})
    as_jsonstr = json.dumps(output)
    
    fh = boto3.client("firehose", "us-east-2")
    fh.put_record(
        DeliveryStreamName="delivery-system", 
        Record={"Data": as_jsonstr.encode('utf-8')})
    return {
        'statusCode': 200,
        'body': json.dumps(f'Done: {as_jsonstr}')}
