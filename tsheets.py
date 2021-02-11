import requests, boto3, os
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO

def tsheets(token, bucket, customfield = ''):

    # -------- CREATE TABLE OF USERS --------- #
    url = 'https://rest.tsheets.com/api/v1/users'
    payload = ''
    headers = {
        'Authorization': 'Bearer {}'.format(token)
        }

    response = requests.request('GET', url, data = payload, headers = headers)
    users = response.json()['results']['users']

    users_dict = {
        'user_id': [],
        'first_name': [],
        'last_name': []
        }

    ids = list(users.keys())

    for i in ids:
        users_dict['user_id'].append(users[i]['id'])
        users_dict['first_name'].append(users[i]['first_name'])
        users_dict['last_name'].append(users[i]['last_name'])

    users = pd.DataFrame(users_dict)

    # -------- CREATE TABLE OF JOBS (CUSTOMERS) --------- #

    url = 'https://rest.tsheets.com/api/v1/jobcodes'

    page = 1
    jobs = []

    while True:
        querystring = {
            'page': page,
            }
        response = requests.request('GET', url, data = payload, headers = headers, params = querystring).json()['results']['jobcodes']

        if len(response):
            jobs.append(response)
        else:
            break
        page += 1
        
    jobs_dict = {
        'jobcode_id': [],
        'name': [],
        }

    for i in jobs:
        ids = list(i.keys())
        for idx in ids:
            jobs_dict['jobcode_id'].append(i[idx]['id'])
            jobs_dict['name'].append(i[idx]['name'])

    jobs = pd.DataFrame(jobs_dict)

    # -------- CREATE TABLE OF TIMESHEETS --------- #

    url = 'https://rest.tsheets.com/api/v1/timesheets'

    page = 1
    timesheets = []

    start_date = str(datetime.today() - timedelta(weeks = 26)).split(' ')[0]
    end_date = str(datetime.today()).split(' ')[0]

    while True:
        querystring = {
            'start_date': start_date,
            'end_date': end_date,
            'page': page,
            }
        response = requests.request('GET', url, data = payload, headers = headers, params = querystring).json()['results']['timesheets']

        if len(response):
                timesheets.append(response)
        else:
            break
        page += 1


    timesheets_dict = {
        'id': [],
        'user_id': [],
        'jobcode_id': [],
        'duration': [],
        'date': [],
        'product': [],
        }

    failures = []

    for i in timesheets:
        ids = list(i.keys())
        for idx in ids:
            try:
                idv = i[idx]['id']
                user_id = i[idx]['user_id']
                jobcode_id = i[idx]['jobcode_id']
                duration = i[idx]['duration'] / 3600
                date = i[idx]['date']
                if len(customfield) == 0:
                    pass
                else:
                    product = i[idx]['customfields'][customfield]

                timesheets_dict['id'].append(idv)
                timesheets_dict['user_id'].append(user_id)
                timesheets_dict['jobcode_id'].append(jobcode_id)
                timesheets_dict['duration'].append(duration)
                timesheets_dict['date'].append(date)
                if len(customfield) == 0:
                    pass
                else:
                    timesheets_dict['product'].append(product)
            except: 
                failures.append(i[idx])

    timesheets = pd.DataFrame(timesheets_dict)

    timesheets = timesheets.merge(users, how = 'left', on = 'user_id')
    timesheets = timesheets.merge(jobs, how = 'left', on = 'jobcode_id')
    timesheets['name'] = timesheets['name'].fillna('NA')

    name = []
    for i in timesheets['name']:
        name.append(i.replace(',', '').replace("'", ''))

    timesheets['name'] = name

    csv_buffer = StringIO()
    timesheets.to_csv(csv_buffer, index = False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, 'timesheets.csv').put(Body = csv_buffer.getvalue())
