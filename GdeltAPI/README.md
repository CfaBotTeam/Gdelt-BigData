# REST API server
## Description

This project implements a python based REST API server. It uses Cassandra as db-backend and REDIS for key-value storage. This is intended for big data queries.

This project is under progress and APIs provided are work in progress and subject to changes.

## Install
To install python dependencies, please run:
```
  pip install -r requirements.txt
```

Make sure you run Cassandra and redis in background.

## Usage

###Load the data
Run the following command to load the data
```
python sync.db
```
This commands load the data presented in data folder into cassandra db. It creates 7 tables as mentioned in the report. Tables are then used for queries.

### Run API server
Run the following command to run the server
```
python main.py
```

This hosts an API server on port 8081.

## API Endpoints

`/user`
`/users`
`/counts`
`/activity`
`/subscribe`
`/summary`

## API documentation
REST APIs are documented in the code

## Examples

Find User by Id

`POST 'http://localhost:8081/user' -d { 'id': 2 }`

Find User By Email

`POST 'http://localhost:8081/user'  -d { 'email': 'test@gmail.com'}`

List Users for a business location

`POST 'http://localhost:8081/users' -d { 'city' : 'Seattle', company: 'Amazon' }`

Report number of users per email domain and city

`POST 'http://localhost:8081/counts' -d { 'domain' : 'mail.ru', 'city': 'San Francisco' }`

Get count of users with activity on a particular day and work for Google

`POST 'http://localhost:8081/activity' -d {'date': '2017/03/02', company: 'Google' }`

Get all users who have subscribed to spotify.com on a given date

`POST 'http://localhost:8081/subscribe' -d {'date': '2017/03/02', 'domain': 'spotify.com', 'event': 'subscribe' }`

Get event type counts by company for a particular timeframe

`POST 'http://localhost:8081/summary' -d {'date' : '2017/03/02', 'company': 'Apple' }`

## Architecture

Database tables are designed to support query in effective manner. Technically data storage is cheap but joining big tables is costly. So we decided to duplicate the data in multiple tables lowering the query time but with little increase in insertion time.

Here are some screenshots for DB tables.
![screen shot 2017-05-01 at 11 26 45 am](https://cloud.githubusercontent.com/assets/14156410/25586081/54f405dc-2e63-11e7-81ba-c5b72ed78924.png)
![screen shot 2017-05-01 at 11 26 58 am](https://cloud.githubusercontent.com/assets/14156410/25586084/57ebf484-2e63-11e7-8e5f-7a75cf0005c9.png)
![screen shot 2017-05-01 at 11 27 10 am](https://cloud.githubusercontent.com/assets/14156410/25586085/57f90f3e-2e63-11e7-9147-6bae226ab208.png)
![screen shot 2017-05-01 at 11 27 34 am](https://cloud.githubusercontent.com/assets/14156410/25586087/57fba6c2-2e63-11e7-9a07-9590bf7c6de5.png)
![screen shot 2017-05-01 at 11 27 54 am](https://cloud.githubusercontent.com/assets/14156410/25586086/57fa6532-2e63-11e7-99bf-052aabd44562.png)
![screen shot 2017-05-01 at 11 27 58 am](https://cloud.githubusercontent.com/assets/14156410/25586088/57ffe296-2e63-11e7-9369-9e6877a2b9b4.png)


