import json
from datetime import datetime

from flask import Flask, abort, make_response
from flask import request
from flask.json import jsonify
from flask_cors import CORS

from api_flights import ApiFlights

app = Flask(__name__)
CORS(app)
api_flights = ApiFlights()


@app.route('/get_airline', methods=['POST'])
def get_airline():
    icao = request.json.get('icao')
    iata = request.json.get('iata')
    name = request.json.get('name')
    country_name = request.json.get('country_name')
    path_url_airline = request.json.get('path_url_airline')
    limit = request.json.get('limit')
    res = api_flights.get_airline(icao=icao, iata=iata, wikidata_info=None, name=name, country_name=country_name,
                                  path_url_airline=path_url_airline, limit=limit)
    return jsonify(res)


@app.route('/get_aircraft', methods=['POST'])
def get_aircraft():
    aircraft_code = request.json.get('aircraft_code')
    url_aircraft = request.json.get('url_aircraft')
    airline_code = request.json.get('airline_code')
    active = request.json.get('active')
    date_updated = request.json.get('date_updated')
    res = api_flights.get_aircraft(aircraft_code=aircraft_code, url_aircraft=url_aircraft, airline_code=airline_code,
                                   active=active,
                                   date_updated=date_updated)
    return jsonify(res)


@app.route('/get_aircraft_family', methods=['POST'])
def get_aircraft_family():
    aircraft_manufacturer = request.json.get('aircraft_manufacturer')
    aircraft_family = request.json.get('aircraft_family')
    res = api_flights.get_aircraft_family(aircraft_manufacturer=aircraft_manufacturer, aircraft_family=aircraft_family)
    return jsonify(res)


@app.route('/get_airport', methods=['POST'])
def get_airport():
    iata = request.json.get('iata')
    name = request.json.get('name')
    country = request.json.get('country')
    city_name = request.json.get('city_name')
    city_id = request.json.get('city_id')
    hub_category = request.json.get('hub_category')
    wikidata_info = request.json.get('wikidata_info')
    limit = request.json.get('limit')

    res = api_flights.get_airport(iata=iata, name=name, country=country, city_name=city_name, hub_category=hub_category,
                                  wikidata_info=wikidata_info, city_id=city_id, limit=limit)

    return jsonify(res)


@app.route('/get_flight_history', methods=['POST'])
def get_flight_history():
    code_flight = request.json.get('code_flight')
    date = request.json.get('date')
    aircraft_code = request.json.get('aircraft_code')
    code_airport_orig = request.json.get('code_airport_orig')
    code_airport_dest = request.json.get('code_airport_dest')
    std = request.json.get('std')
    status = request.json.get('status')
    limit = request.json.get('limit')
    res = api_flights.get_flight_history(code_flight=code_flight, date=date, aircraft_code=aircraft_code, std=None,
                                         code_airport_orig=code_airport_orig, status=status, limit=limit)
    return jsonify(res)


@app.route('/get_flight', methods=['POST'])
def get_flight():
    code_flight = request.json.get('code_flight')
    airline_code = request.json.get('airline_code')
    code_airport_orig = request.json.get('code_airport_orig')
    code_airport_dest = request.json.get('code_airport_dest')
    std = request.json.get('std')
    limit = request.json.get('limit')
    res = api_flights.get_flight(code_flight=code_flight, airline_code=airline_code, std=std,
                                 code_airport_orig=code_airport_orig, code_airport_dest=code_airport_dest, limit=limit)
    return jsonify(res)


@app.route('/get_flight_price', methods=['POST'])
def get_flight_price():
    airline_code = request.json.get('airline_code')
    code_flight = request.json.get('code_flight')
    code_airport_orig = request.json.get('code_airport_orig')
    code_airport_dest = request.json.get('code_airport_dest')
    departure_date = request.json.get('departure_date')
    std = request.json.get('std')
    limit = request.json.get('limit')
    res = api_flights.get_flight_price(code_flight=code_flight, departure_date=departure_date,
                                       code_airport_orig=code_airport_orig, code_airport_dest=code_airport_dest,
                                       std=std, limit=limit)
    return jsonify(res)


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=3000)
