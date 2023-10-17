# Call external libraries
import psycopg2
import locale
from flask import Flask, jsonify, abort, make_response, request

# Set locale to Spanish
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

# Create default Flask application
app = Flask(__name__)

# Define conex as a global variable
conex = None

# ================================================================
# D A T A   A C C E S S   C O D E
# ================================================================

# Function to execute data modification sentence
def execute(auxsql):
    global conex  # Declare conex as global
    data = None
    try:
        # Create data access object
        conex = psycopg2.connect(
            host='192.168.0.27',
            database='demo',
            user='postgres',
            password='Parda99$'
        )
        # Create a local cursor for SQL execution
        cur = conex.cursor()
        # Execute SQL sentence
        cur.execute(auxsql)
        # Retrieve data if it exists
        data = cur.fetchall()
        # Close cursor
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conex is not None:
            conex.close()
            print('Connection closed.')
    # Return data
    return data

# ================================================================
# A P I   R E S T F U L   S E R V I C E
# ================================================================

# -----------------------------------------------------
# Error support section
# -----------------------------------------------------

@app.errorhandler(400)
def bad_request(error):
    return make_response(jsonify({'error': 'Bad request....!'}), 400)

@app.errorhandler(401)
def unauthorized(error):
    return make_response(jsonify({'error': 'Unauthorized....!'}), 401)

@app.errorhandler(403)
def forbidden(error):
    return make_response(jsonify({'error': 'Forbidden....!'}), 403)

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found....!'}), 404)

# Get Aircraft
@app.route('/aircraft', methods=['GET'])
def get_aircraft():
    # Obtener el idioma seleccionado por el usuario desde los parámetros de la solicitud
    idioma = request.args.get('idioma', 'en')  # Predeterminado a 'en' si no se proporciona

    # Mapa de idiomas admitidos a las claves JSON en la base de datos
    idioma_map = {
        'en': 'en',  # Inglés
        'ru': 'ru'  # Ruso (agrega idioma ruso)
    }

    idioma_seleccionado = idioma_map.get(idioma, 'en')  # Obtener la clave de idioma correspondiente

    # Consulta SQL con el idioma seleccionado
    sql_query = f"""
    SELECT
        ad.aircraft_code,
        ad.model->>'{idioma_seleccionado}' AS "model",
        ad.range
    FROM aircrafts_data ad
    """

    resu = execute(sql_query)

    if resu is not None:
        salida = {
            "status_code": 200,
            "status": "OK",
            "data": []
        }
        for cod, modelo, rango in resu:
            salida["data"].append({
                "code": cod,
                "model": modelo,
                "range": rango
            })
    else:
        abort(404)

    return jsonify({'data': salida}), 200


@app.route('/punto1', methods=['GET'])
def get_airports_data():
    # Obtener el valor del parámetro 'idioma' de la URL
    idioma = request.args.get('idioma', 'en')  # Predeterminado a 'en' si no se proporciona

    # Mapa de idiomas admitidos a las claves JSON en la base de datos
    idioma_map = {
        'en': 'en',  # Inglés
        'ru': 'ru'  # Ruso (agrega idioma ruso)
    }

    idioma_seleccionado = idioma_map.get(idioma, 'en')  # Obtener la clave de idioma correspondiente

    # Consulta SQL con el idioma seleccionado
    sql_query = f"""
    SELECT
        airport_code AS "Código",
        airport_name->>'{idioma_seleccionado}' AS "Nombre del aeropuerto",
        city->>'{idioma_seleccionado}' AS "Nombre de la ciudad",
        coordinates[0] AS "Longitud",
        coordinates[1] AS "Latitud",
        timezone AS "Zona horaria"
    FROM bookings.airports_data
    LIMIT 10
    """

    resu = execute(sql_query)

    if resu is not None:
        salida = {
            "status_code": 200,
            "status": "OK",
            "data": []
        }
        for (
                codigo_aeropuerto, nombre_aeropuerto, nombre_ciudad, longitud,
                latitud, zona_horaria
        ) in resu:
            salida["data"].append({
                "Código": codigo_aeropuerto,
                "Nombre del aeropuerto": nombre_aeropuerto,
                "Nombre de la ciudad": nombre_ciudad,
                "Longitud": longitud,
                "Latitud": latitud,
                "Zona horaria": zona_horaria,
            })
    else:
        abort(404)

    return jsonify({'data': salida}), 200

# Get Aircraft
@app.route('/punto2', methods=['GET'])
def get_flights_data():
    resu = execute(
        """
        SELECT
            f.flight_no AS "Número de vuelo",
            dep_airport.airport_name AS "Nombre del aeropuerto de salida",
            arr_airport.airport_name AS "Nombre del aeropuerto de llegada",
            f.scheduled_departure AS "Hora oficial de salida",
            f.scheduled_arrival AS "Hora oficial de llegada",
            t.passenger_id AS "Id del pasajero",
            t.passenger_name AS "Nombre del pasajero",
            tf.fare_conditions AS "Clase en la que viajó el pasajero",
            t.contact_data->>'email' AS "Email",
            t.contact_data->>'phone' AS "Teléfono"
        FROM bookings.flights f
        INNER JOIN bookings.airports_data dep_airport ON f.departure_airport = dep_airport.airport_code
        INNER JOIN bookings.airports_data arr_airport ON f.arrival_airport = arr_airport.airport_code
        INNER JOIN bookings.boarding_passes bp ON bp.flight_id = f.flight_id
        INNER JOIN bookings.ticket_flights tf ON bp.ticket_no = tf.ticket_no AND bp.flight_id = tf.flight_id
        INNER JOIN bookings.tickets t ON tf.ticket_no = t.ticket_no
        LIMIT 10
        """
    )
    if resu is not None:
        salida = {
            "status_code": 200,
            "status": "OK",
            "data": []
        }
        for (
            flight_no, airport_salida, airport_llegada, scheduled_departure,
            scheduled_arrival, passenger_id, passenger_name, fare_conditions, email, phone
        ) in resu:
            salida["data"].append({
                "Número de vuelo": flight_no,
                "Nombre del aeropuerto de salida": airport_salida,
                "Nombre del aeropuerto de llegada": airport_llegada,
                "Hora oficial de salida": scheduled_departure,
                "Hora oficial de llegada": scheduled_arrival,
                "Id del pasajero": passenger_id,
                "Nombre del pasajero": passenger_name,
                "Clase en la que viajó el pasajero": fare_conditions,
                "Email": email,
                "Teléfono": phone,
            })
    else:
        abort(404)
    return jsonify({'data': salida}), 200

@app.route('/punto3', methods=['GET'])
def get_flights_occupation():
    resu = execute(
        """
        SELECT
            f.flight_id AS "ID de vuelo",
            s.fare_conditions AS "Tipo de clase",
            COUNT(bp.seat_no) AS "Cantidad de sillas ocupadas",
            (
                SELECT COUNT(*) 
                FROM bookings.seats 
                WHERE seats.aircraft_code = f.aircraft_code 
                AND seats.fare_conditions = s.fare_conditions
            ) - COUNT(bp.seat_no) AS "Cantidad de sillas disponibles",
            (
                SELECT COUNT(*) 
                FROM bookings.seats 
                WHERE seats.aircraft_code = f.aircraft_code 
                AND seats.fare_conditions = s.fare_conditions
            ) AS "Total de sillas"
        FROM bookings.boarding_passes bp
        INNER JOIN bookings.ticket_flights tf 
        ON bp.ticket_no = tf.ticket_no AND bp.flight_id = tf.flight_id
        INNER JOIN bookings.flights f 
        ON bp.flight_id = f.flight_id
        INNER JOIN bookings.seats s 
        ON f.aircraft_code = s.aircraft_code AND bp.seat_no = s.seat_no
        GROUP BY f.flight_id, s.fare_conditions
        LIMIT 10
        """
    )
    if resu is not None:
        salida = {
            "status_code": 200,
            "status": "OK",
            "data": []
        }
        for (
            flight_id, fare_conditions, sillas_ocupadas, sillas_disponibles, total_sillas
        ) in resu:
            salida["data"].append({
                "ID de vuelo": flight_id,
                "Tipo de clase": fare_conditions,
                "Cantidad de sillas ocupadas": sillas_ocupadas,
                "Cantidad de sillas disponibles": sillas_disponibles,
                "Total de sillas": total_sillas,
            })
    else:
        abort(404)
    return jsonify({'data': salida}), 200



# -----------------------------------------------------
# Create the Flask app
# -----------------------------------------------------

if __name__ == '__main__':
    app.run(host='localhost', port=5001, debug=True)
