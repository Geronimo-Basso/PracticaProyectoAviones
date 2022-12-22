import time
from time import time

import psycopg2
import psycopg2.extras


class ApiFlights(object):
    def __init__(self):
        self.conn = psycopg2.connect(host='127.0.0.1',
                                     port=5432,
                                     user='postgres',
                                     password='1234',
                                     database='flights')
        # Database Credentials
        self.conn.autocommit = True

    def update_airport(self, iata, lat=None, lon=None, name=None, country=None, comercial=None, city_name=None,
                       hub_category=None, wikidata_info=None, website=None, area=None, date=None, city_id=None,
                       runways=None):
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        if name:
            cursor.execute("""
            UPDATE flights.airport SET name = %s
            WHERE iata = %s
            """, (name, iata))
        if country:
            cursor.execute("""
            UPDATE flights.airport SET country_name = %s
            WHERE iata = %s
            """, (country, iata))
        if lat:
            cursor.execute("""
            UPDATE flights.airport SET lat = %s
            WHERE iata = %s
            """, (lat, iata))
        if lon:
            cursor.execute("""
            UPDATE flights.airport SET lon = %s
            WHERE iata = %s
            """, (lon, iata))
        if city_name:
            cursor.execute("""
            UPDATE flights.airport SET city_name = %s
            WHERE iata = %s
            """, (city_name, iata))
        if city_id:
            cursor.execute("""
            UPDATE flights.airport SET city_id = %s
            WHERE iata = %s
            """, (city_id, iata))

        if hub_category:
            cursor.execute("""
            UPDATE flights.airport SET hub_category = %s
            WHERE iata = %s
            """, (hub_category, iata))
        if wikidata_info:
            cursor.execute("""
            UPDATE flights.airport SET wikidata_info = %s
            WHERE iata = %s
            """, (wikidata_info, iata))
        if website:
            cursor.execute("""
            UPDATE flights.airport SET website = %s
            WHERE iata = %s
            """, (website, iata))
        if area:
            cursor.execute("""
            UPDATE flights.airport SET area = %s
            WHERE iata = %s
            """, (area, iata))
        if date:
            cursor.execute("""
            UPDATE flights.airport SET date = %s
            WHERE iata = %s
            """, (date, iata))
        if runways:
            cursor.execute("""
            UPDATE flights.airport SET runways = %s
            WHERE iata = %s
            """, (runways, iata))
        cursor.close()
        self.conn.commit()

    def update_airline(self, icao, name=None, country_name=None, website=None, wikidata_info=None, creation_date=None,
                       alliance=None, comercial=None, location_formation=None, parent_organization=None, employees=None,
                       facebook_name=None, instagram_name=None, twitter_name=None, linkedin_name=None):
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        if wikidata_info:
            cursor.execute("""
            UPDATE flights.airline SET wikidata_info = %s
            WHERE icao = %s
            """, (wikidata_info, icao))
        if name:
            cursor.execute("""
            UPDATE flights.airline SET name = %s
            WHERE icao = %s
            """, (name, icao))
        if country_name:
            cursor.execute("""
            UPDATE flights.airline SET country_name = %s
            WHERE icao = %s
            """, (country_name, icao))
        if creation_date:
            cursor.execute("""
            UPDATE flights.airline SET creation_date = %s
            WHERE icao = %s
            """, (creation_date, icao))
        if alliance:
            cursor.execute("""
            UPDATE flights.airline SET alliance = %s
            WHERE icao = %s
            """, (alliance, icao))
        if location_formation:
            cursor.execute("""
            UPDATE flights.airline SET location_formation = %s
            WHERE icao = %s
            """, (location_formation, icao))
        if parent_organization:
            cursor.execute("""
            UPDATE flights.airline SET parent_organization = %s
            WHERE icao = %s
            """, (parent_organization, icao))
        if website:
            cursor.execute("""
            UPDATE flights.airline SET website = %s
            WHERE icao = %s
            """, (website, icao))
        if employees:
            cursor.execute("""
            UPDATE flights.airline SET employees = %s
            WHERE icao = %s
            """, (employees, icao))
        if facebook_name:
            cursor.execute("""
            UPDATE flights.airline SET facebook_name = %s
            WHERE icao = %s
            """, (facebook_name, icao))
        if instagram_name:
            cursor.execute("""
            UPDATE flights.airline SET instagram_name = %s
            WHERE icao = %s
            """, (instagram_name, icao))
        if twitter_name:
            cursor.execute("""
            UPDATE flights.airline SET twitter_name = %s
            WHERE icao = %s
            """, (twitter_name, icao))
        if linkedin_name:
            cursor.execute("""
            UPDATE flights.airline SET linkedin_name = %s
            WHERE icao = %s
            """, (linkedin_name, icao))
        if comercial is not None:
            cursor.execute("""
            UPDATE flights.airline SET comercial = %s
            WHERE icao = %s
            """, (comercial, icao))
        cursor.close()
        self.conn.commit()

    def update_aircraft(self, aircraft_code, aircraft_family=None, airline_code=None, active=None, updated=None,
                        date_inserted=None):
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        if airline_code:
            cursor.execute("""
            UPDATE flights.aircraft SET airline_code = %s
            WHERE aircraft_code = %s
            """, (airline_code, aircraft_code))
        if active is not None:
            cursor.execute("""
            UPDATE flights.aircraft SET active = %s
            WHERE aircraft_code = %s
            """, (active, aircraft_code))
        if aircraft_family is not None:
            cursor.execute("""
            UPDATE flights.aircraft SET aircraft_family = %s
            WHERE aircraft_code = %s
            """, (aircraft_family, aircraft_code))
        if updated is not None:
            cursor.execute("""
            UPDATE flights.aircraft SET updated = %s
            WHERE aircraft_code = %s
            """, (updated, aircraft_code))
        if date_inserted is not None:
            cursor.execute("""
            UPDATE flights.aircraft SET date_inserted = %s
            WHERE aircraft_code = %s
            """, (date_inserted, aircraft_code))
        cursor.close()
        self.conn.commit()

    def update_aircraft_family(self, aircraft_family, wing_span=None, overall_length=None, height=None, mmo=None,
                               fuel_capacity=None, pax_seating=None, pax_seating_2_class=None, n_motors=None,
                               takeoff_distance=None, landing_distance=None, absolute_ceiling=None,
                               optimum_ceiling=None,
                               maximum_speed=None, optimum_speed=None, max_weight=None, range=None,
                               maximum_climb_rate=None, cabin_length=None, fuselage_width=None, max_cabin_width=None):
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        if wing_span:
            cursor.execute("""
            UPDATE flights.aircraft_family SET wing_span = %s
            WHERE aircraft_family = %s
            """, (wing_span, aircraft_family))
        if overall_length:
            cursor.execute("""
            UPDATE flights.aircraft_family SET overall_length = %s
            WHERE aircraft_family = %s
            """, (overall_length, aircraft_family))
        if height:
            cursor.execute("""
            UPDATE flights.aircraft_family SET height = %s
            WHERE aircraft_family = %s
            """, (height, aircraft_family))
        if cabin_length:
            cursor.execute("""
            UPDATE flights.aircraft_family SET cabin_length = %s
            WHERE aircraft_family = %s
            """, (cabin_length, aircraft_family))
        if fuselage_width:
            cursor.execute("""
            UPDATE flights.aircraft_family SET fuselage_width = %s
            WHERE aircraft_family = %s
            """, (fuselage_width, aircraft_family))
        if max_cabin_width:
            cursor.execute("""
            UPDATE flights.aircraft_family SET max_cabin_width = %s
            WHERE aircraft_family = %s
            """, (max_cabin_width, aircraft_family))
        if mmo:
            cursor.execute("""
            UPDATE flights.aircraft_family SET mmo = %s
            WHERE aircraft_family = %s
            """, (mmo, aircraft_family))
        if fuel_capacity:
            cursor.execute("""
            UPDATE flights.aircraft_family SET fuel_capacity = %s
            WHERE aircraft_family = %s
            """, (fuel_capacity, aircraft_family))
        if pax_seating:
            cursor.execute("""
            UPDATE flights.aircraft_family SET pax_seating = %s
            WHERE aircraft_family = %s
            """, (pax_seating, aircraft_family))
        if pax_seating_2_class:
            cursor.execute("""
            UPDATE flights.aircraft_family SET pax_seating_2_class = %s
            WHERE aircraft_family = %s
            """, (pax_seating_2_class, aircraft_family))
        if n_motors:
            cursor.execute("""
            UPDATE flights.aircraft_family SET n_motors = %s
            WHERE aircraft_family = %s
            """, (n_motors, aircraft_family))
        if takeoff_distance:
            cursor.execute("""
            UPDATE flights.aircraft_family SET takeoff_distance = %s
            WHERE aircraft_family = %s
            """, (takeoff_distance, aircraft_family))
        if landing_distance:
            cursor.execute("""
            UPDATE flights.aircraft_family SET landing_distance = %s
            WHERE aircraft_family = %s
            """, (landing_distance, aircraft_family))
        if absolute_ceiling:
            cursor.execute("""
            UPDATE flights.aircraft_family SET absolute_ceiling = %s
            WHERE aircraft_family = %s
            """, (absolute_ceiling, aircraft_family))
        if optimum_ceiling:
            cursor.execute("""
            UPDATE flights.aircraft_family SET optimum_ceiling = %s
            WHERE aircraft_family = %s
            """, (optimum_ceiling, aircraft_family))
        if maximum_speed:
            cursor.execute("""
            UPDATE flights.aircraft_family SET maximum_speed = %s
            WHERE aircraft_family = %s
            """, (maximum_speed, aircraft_family))
        if optimum_speed:
            cursor.execute("""
            UPDATE flights.aircraft_family SET optimum_speed = %s
            WHERE aircraft_family = %s
            """, (optimum_speed, aircraft_family))
        if max_weight:
            cursor.execute("""
            UPDATE flights.aircraft_family SET max_weight = %s
            WHERE aircraft_family = %s
            """, (max_weight, aircraft_family))
        if range:
            cursor.execute("""
            UPDATE flights.aircraft_family SET range = %s
            WHERE aircraft_family = %s
            """, (range, aircraft_family))
        if maximum_climb_rate:
            cursor.execute("""
            UPDATE flights.aircraft_family SET maximum_climb_rate = %s
            WHERE aircraft_family = %s
            """, (maximum_climb_rate, aircraft_family))

        cursor.close()
        self.conn.commit()

    def update_flight_history(self, code_flight, date, code_airport_orig, std, code_airport_dest=None, std_change=None,
                              atd=None, sta=None, ata=None, status=None):
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        if code_airport_dest:
            cursor.execute("""
            UPDATE flights.flight_history SET code_airport_dest = %s
            WHERE code_flight = %s and date = %s and code_airport_orig = %s and std = %s
            """, (code_airport_dest, code_flight, date, code_airport_orig, std))
        if std_change:
            cursor.execute("""
            UPDATE flights.flight_history SET std = %s
            WHERE code_flight = %s and date = %s and code_airport_orig = %s and std = %s
            """, (std_change, code_flight, date, code_airport_orig, std))
        if atd:
            cursor.execute("""
            UPDATE flights.flight_history SET atd = %s
            WHERE code_flight = %s and date = %s and code_airport_orig = %s and std = %s
            """, (atd, code_flight, date, code_airport_orig, std))
        if sta:
            cursor.execute("""
            UPDATE flights.flight_history SET sta = %s
            WHERE code_flight = %s and date = %s and code_airport_orig = %s and std = %s
            """, (sta, code_flight, date, code_airport_orig, std))
        if ata:
            cursor.execute("""
            UPDATE flights.flight_history SET ata = %s
            WHERE code_flight = %s and date = %s and code_airport_orig = %s and std = %s
            """, (ata, code_flight, date, code_airport_orig, std))
        if status:
            cursor.execute("""
            UPDATE flights.flight_history SET status = %s
            WHERE code_flight = %s and date = %s and code_airport_orig = %s and std = %s
            """, (status, code_flight, date, code_airport_orig, std))

        cursor.close()
        self.conn.commit()

    def get_flight(self, airline_code=None, code_flight=None, code_airport_orig=None, code_airport_dest=None,
                   std=None, limit=None):
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        select_query = "SELECT * FROM flights.flight "
        where_query = None
        limit_query = " "
        if limit:
            limit_query = "limit {} ".format(limit)
        if airline_code:
            where_query = "WHERE airline_code='{}' ".format(airline_code)
        if code_flight and not where_query:
            where_query = "WHERE code_flight='{}' ".format(code_flight)
        elif code_flight and where_query:
            where_query += "AND code_flight='{}' ".format(code_flight)
        if code_airport_orig and not where_query:
            where_query = "WHERE code_airport_orig='{}' ".format(code_airport_orig)
        elif code_airport_orig and where_query:
            where_query += "AND code_airport_orig='{}' ".format(code_airport_orig)
        if code_airport_dest and not where_query:
            where_query = "WHERE code_airport_dest='{}' ".format(code_airport_dest)
        elif code_airport_dest and where_query:
            where_query += "AND code_airport_dest='{}' ".format(code_airport_dest)
        if std and not where_query:
            where_query = "WHERE std='{}' ".format(std)
        elif std and where_query:
            where_query += "AND std='{}' ".format(std)
        if not where_query:
            where_query = " "
        cursor.execute(select_query + where_query + limit_query)
        res = cursor.fetchall()
        res = [dict(x) for x in res]

        cursor.close()
        return res

    def get_aircraft(self, aircraft_code=None, url_aircraft=None, airline_code=None, active=None, date_updated=None):
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        select_query = "SELECT * FROM flights.aircraft "
        where_query = None
        order_query = "ORDER BY aircraft_code ASC"

        if aircraft_code:
            where_query = "WHERE aircraft_code='{}' ".format(aircraft_code)
        if url_aircraft and not where_query:
            where_query = "WHERE aircraft_url='{}' ".format(url_aircraft)
        elif url_aircraft and where_query:
            where_query += "AND aircraft_url='{}' ".format(url_aircraft)
        if date_updated and not where_query:
            where_query = "WHERE updated='{}' ".format(date_updated)
        elif date_updated and where_query:
            where_query += "AND updated='{}' ".format(date_updated)
        if airline_code and not where_query:
            where_query = "WHERE UPPER(airline_code)='{}' ".format(airline_code.upper())
        elif airline_code and where_query:
            where_query += "AND UPPER(airline_code)='{}' ".format(airline_code.upper())
        if active is not None and not where_query:
            where_query = "WHERE active is {} ".format(active)
        elif active is not None and where_query:
            where_query += "AND active is {} ".format(active)
        if not where_query:
            where_query = ' '
        cursor.execute(select_query + where_query + order_query)
        res = cursor.fetchall()
        res = [dict(x) for x in res]

        cursor.close()
        return res

    def get_aircraft_family(self, aircraft_manufacturer=None, aircraft_family=None):
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        select_query = "SELECT * FROM flights.aircraft_family "
        where_query = None
        order_query = "ORDER BY aircraft_manufacturer ASC, aircraft_family ASC"

        if aircraft_manufacturer:
            where_query = "WHERE aircraft_manufacturer='{}' ".format(aircraft_manufacturer)
        if aircraft_family and not where_query:
            where_query = "WHERE aircraft_family='{}' ".format(aircraft_family)
        elif aircraft_family and where_query:
            where_query += "AND aircraft_family='{}' ".format(aircraft_family)

        if not where_query:
            where_query = ' '
        cursor.execute(select_query + where_query + order_query)
        res = cursor.fetchall()
        res = [dict(x) for x in res]

        cursor.close()
        return res

    def get_flight_history(self, code_flight=None, date=None, std=None, aircraft_code=None, code_airport_orig=None,
                           limit=None, status=None, code_airport_dest=None, ata=None):
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        select_query = "SELECT * FROM flights.flight_history "
        where_query = None
        order_query = "ORDER BY date DESC, std DESC "
        limit_query = " "
        if limit is not None:
            limit_query = f"LIMIT {limit}"

        if code_flight:
            where_query = "WHERE code_flight='{}' ".format(code_flight)
        if date and not where_query:
            where_query = "WHERE date='{}' ".format(date)
        elif date and where_query:
            where_query += "AND date='{}' ".format(date)
        if std and not where_query:
            where_query = "WHERE std='{}' ".format(std)
        elif std and where_query:
            where_query += "AND std='{}' ".format(std)
        if aircraft_code and not where_query:
            where_query = "WHERE aircraft_code='{}' ".format(aircraft_code)
        elif aircraft_code and where_query:
            where_query += "AND aircraft_code='{}' ".format(aircraft_code)
        if code_airport_orig and not where_query:
            where_query = "WHERE code_airport_orig='{}' ".format(code_airport_orig)
        elif code_airport_orig and where_query:
            where_query += "AND code_airport_orig='{}' ".format(code_airport_orig)
        if status and not where_query:
            where_query = "WHERE status='{}' ".format(status)
        elif status and where_query:
            where_query += "AND status='{}' ".format(status)
        if code_airport_dest and not where_query:
            where_query = "WHERE code_airport_dest='{}' ".format(code_airport_dest)
        elif code_airport_dest and where_query:
            where_query += "AND code_airport_dest='{}' ".format(code_airport_dest)

        if not where_query:
            where_query = ' '
        cursor.execute(select_query + where_query + order_query + limit_query)
        res = cursor.fetchall()
        res = [dict(x) for x in res]

        cursor.close()
        return res

    def get_airline(self, icao=None, iata=None, wikidata_info=None, name=None, country_name=None,
                    path_url_airline=None, limit=None):
        init_time = time()
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        select_query = "SELECT DISTINCT * from flights.airline "
        where_query = None
        order_query = "ORDER BY iata ASC "
        limit_query = " "
        if limit is not None:
            limit_query = f"LIMIT {limit}"
        if iata:
            where_query = "WHERE iata='{}' ".format(iata)
        if name:
            if where_query:
                where_query += "AND name='{}' ".format(name)
            else:
                where_query = "WHERE name='{}' ".format(name)
        if icao:
            if where_query:
                where_query += "AND icao='{}' ".format(icao)
            else:
                where_query = "WHERE icao='{}' ".format(icao)
        if name:
            if where_query:
                where_query += "AND name='{}' ".format(name)
            else:
                where_query = "WHERE name='{}' ".format(name)
        if country_name:
            if where_query:
                where_query += "AND country_name='{}' ".format(country_name)
            else:
                where_query = "WHERE country_name='{}' ".format(country_name)
        if wikidata_info:
            if where_query:
                where_query += "AND wikidata_info='{}' ".format(wikidata_info)
            else:
                where_query = "WHERE wikidata_info='{}' ".format(wikidata_info)
        if path_url_airline:
            if where_query:
                where_query += "AND path_url_airline='{}' ".format(path_url_airline)
            else:
                where_query = "WHERE path_url_airline='{}' ".format(path_url_airline)
        if not where_query:
            where_query = ''
        cursor.execute(select_query + where_query + order_query + limit_query)
        res = cursor.fetchall()
        res = [dict(x) for x in res]

        cursor.close()
        return res

    def get_airport(self, iata=None, name=None, country=None, city_name=None, hub_category=None, wikidata_info=None,
                    city_id=None, limit=None):
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        select_query = "SELECT * FROM flights.airport "
        where_query = None
        order_query = "ORDER BY hub_category ASC "
        limit_query = " "
        if limit is not None:
            limit_query = f"LIMIT {limit}"
        if iata:
            where_query = "WHERE iata='{}' ".format(iata)
        if name and where_query:
            where_query += "AND name='{}' ".format(name)
        elif name and not where_query:
            where_query = "WHERE name='{}' ".format(name)
        if country and where_query:
            where_query += "AND country_name='{}' ".format(country)
        elif country and not where_query:
            where_query = "WHERE country_name='{}' ".format(country)
        if city_name and where_query:
            where_query += "AND city_name='{}' ".format(city_name)
        elif city_name and not where_query:
            where_query = "WHERE city_name='{}' ".format(city_name)
        if city_id and where_query:
            where_query += "AND city_id='{}' ".format(city_id)
        elif city_id and not where_query:
            where_query = "WHERE city_id='{}' ".format(city_id)
        if hub_category and where_query:
            where_query += "AND hub_category={} ".format(hub_category)
        elif hub_category and not where_query:
            where_query = "WHERE hub_category={} ".format(hub_category)
        if wikidata_info and where_query:
            where_query += "AND wikidata_info='{}' ".format(wikidata_info)
        elif wikidata_info and not where_query:
            where_query = "WHERE wikidata_info='{}' ".format(wikidata_info)
        if not where_query:
            where_query = " "
        cursor.execute(select_query + where_query + order_query + limit_query)
        res = cursor.fetchall()
        res = [dict(x) for x in res]

        cursor.close()
        return res

    def get_flight_price(self, code_flight=None, departure_date=None, code_airport_orig=None, code_airport_dest=None,
                         airline_code=None, std=None, limit=None):
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        select_query = "SELECT * FROM flights.flight_price fp inner join flights.flight f on (fp.code_flight=f.code_flight)"
        where_query = None
        order_query = "ORDER BY fp.price ASC "
        limit_query = " "
        if limit:
            limit_query = f"limit {limit} "
        if code_flight:
            where_query = "WHERE f.code_flight='{}' ".format(code_flight)
        if departure_date and not where_query:
            where_query = "WHERE departure_date='{}' ".format(departure_date)
        elif departure_date and where_query:
            where_query += "AND departure_date='{}' ".format(departure_date)
        if std and not where_query:
            where_query = "WHERE std='{}' ".format(std)
        elif std and where_query:
            where_query += "AND std='{}' ".format(std)
        if code_airport_dest and not where_query:
            where_query = "WHERE code_airport_dest='{}' ".format(code_airport_dest)
        elif code_airport_dest and where_query:
            where_query += "AND code_airport_dest='{}' ".format(code_airport_dest)
        if code_airport_orig and not where_query:
            where_query = "WHERE code_airport_orig='{}' ".format(code_airport_orig)
        elif code_airport_orig and where_query:
            where_query += "AND code_airport_orig='{}' ".format(code_airport_orig)
        if airline_code and not where_query:
            where_query = "WHERE airline_code='{}' ".format(airline_code)
        elif airline_code and where_query:
            where_query += "AND airline_code='{}' ".format(airline_code)

        if not where_query:
            where_query = ' '
        cursor.execute(select_query + where_query + order_query + limit_query)
        res = cursor.fetchall()
        res = [dict(x) for x in res]

        cursor.close()
        return res
