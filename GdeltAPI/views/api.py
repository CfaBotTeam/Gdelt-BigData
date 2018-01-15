from functools import wraps
from views.util import DateTimeEncoder
import json
from flask import Blueprint, Response
import flask
import json

from dao.db import CassandraDBConnector
from codes.sectors_loader import SectorsLoader

__author__ = 'puneet'

api = Blueprint("api",__name__)

db = CassandraDBConnector()
db.connect("ec2-54-160-15-115.compute-1.amazonaws.com", "9042")

sectors_loader = SectorsLoader()
sectors_loader.load_company_sectors()

def json_api(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        result = f(*args, **kwargs)      # call function
        json_result = json.dumps(result, cls=DateTimeEncoder)
        return Response(response=json_result, 
                        status=200,
                        mimetype='application/json')
    return decorated_function


@api.route('/', defaults={"path": ""})
@api.route('/<path:path>')
def index(path=None):
    return "Hello World"


@api.route("/firms", methods=["GET"])
@json_api
def get_nb_firms():
    return sectors_loader.get_companies()


@api.route("/sectors", methods=["GET"])
@json_api
def get_nb_sectors():
    return sectors_loader.get_sectors()


@api.route("/sources", methods=["GET"])
@json_api
def get_sources():
    query = "select distinct mentionsourcename from events_mentions_sourcename"
    result = db.query(query)
    sources = []
    for source in result:
        sources.append(source.mentionsourcename)
    return sources


@api.route("/mentions/average_note", methods=["GET"])
@json_api
def get_average_notes():
    query = "SELECT avg(mentiondoctone) FROM events.events_mentions"
    result = db.query(query)
    return result.was_applied


@api.route("/mentions", methods=["GET"])
@json_api
def get_mentions_by_firm():
    args = flask.request.args
    firm = args['firm']
    startDate = args['startDate']
    endDate = args['endDate']

    if firm is None or startDate is None or endDate is None:
        return {"error": "Invalid Input"}

    sector = sectors_loader.get_sector(firm)
    if sector is None:
        return {"error": "Sector not found for firm: " + firm}

    query = \
        "SELECT * FROM events.events_mentions where sector = '{}' and firm = '{}' " + \
        "and mentionDateTime >= '{}' and mentionDateTime <= '{}'"
    result = db.query(query.format(sector, firm, startDate, endDate))
    mentions = []
    if result is None:
        return {'error': "No User Present"}

    for row in result:
        mentions.append({
            "globalEventsId": row.globaleventsid,
            "firm": row.firm,
            "sector": row.sector,
            "mentionSourceName": row.mentionsourcename,
            "country": row.country,
            "confidence": row.confidence,
            "mentionDocTone": row.mentiondoctone,
            "mentionDateTime": row.mentiondatetime
        })
    return {'mentions': mentions}


# "eventDate": row.eventdate,
