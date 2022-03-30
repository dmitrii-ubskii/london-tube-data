#!/usr/bin/env python3

import json
import readline

import MySQLdb
import MySQLdb.cursors  # for the type hint


def populate_db(conn: MySQLdb.Connection):
    with open('train-network.json') as f:
        data = json.load(f)

    conn.query('CREATE DATABASE London_Tube;')
    conn.select_db('London_Tube')
    conn.query(
        '''CREATE TABLE Stations (
            id VARCHAR(256) NOT NULL PRIMARY KEY,
            name VARCHAR(256) NOT NULL,
            longitude DOUBLE NOT NULL,
            latitude DOUBLE NOT NULL
        );'''
    )

    cursor = conn.cursor()
    cursor.executemany(
        'INSERT INTO Stations (id, name, longitude, latitude) VALUES (%s, %s, %s, %s);',
        [
            (station['id'], station['name'], station['longitude'], station['latitude'])
            for station in data['stations']
        ]
    )

    conn.query(
        '''CREATE TABLE TubeLines (
            id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(256) NOT NULL
        );'''
    )
    cursor.executemany(
        'INSERT INTO TubeLines (name) VALUES (%s);',
        [(line['name'],) for line in data['lines']]
    )
    cursor.execute('SELECT id, name FROM TubeLines;')
    line_ids = {
        name: line_id for line_id, name in cursor.fetchall()
    }

    conn.query(
        '''CREATE TABLE TubeLineStations (
            id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            stationId VARCHAR(256),
            tubeLineId int
        );'''
    )
    cursor.executemany(
        'INSERT INTO TubeLineStations (stationId, tubeLineId) VALUES (%s, %s);',
        [
            (station_id, line_ids[line['name']]) for line in data['lines'] 
            for station_id in line['stations']
        ]
    )


def print_line_stations(line_name: str, cursor: MySQLdb.cursors.Cursor):
    cursor.execute('SELECT id FROM TubeLines WHERE name = %s;', (line_name,))
    result = cursor.fetchone()
    if result is None:
        print(f'ERROR: Line by the name `{line_name}` not found.')
        return

    line_id = result
    cursor.execute('''
        SELECT name
        FROM TubeLineStations JOIN Stations
            ON (TubeLineStations.stationId = Stations.id)
        WHERE tubeLineId = %s;
    ''', (line_id,))
    print(f'Stations on the tube line "{line_name}":')
    for station_name, in cursor.fetchall():
        print(f'    {station_name}')


def print_station_lines(station_name: str, cursor: MySQLdb.cursors.Cursor):
    cursor.execute('SELECT id FROM Stations WHERE name = %s;', (station_name,))
    result = cursor.fetchone()
    if result is None:
        print(f'ERROR: Station by the name `{station_name}` not found.')
        return

    station_id = result
    cursor.execute('''
        SELECT name
        FROM TubeLineStations JOIN TubeLines
            ON (TubeLineStations.tubeLineId = TubeLines.id)
        WHERE stationId = %s;
    ''', (station_id,))
    lines = cursor.fetchall()
    if len(lines) == 1:
        line_name = lines[0][0]
        print(f'The station {station_name} is located on the {line_name} line.')
    else:
        print(f'The station {station_name} is located on the following lines: ', end='')
        print(', '.join(map(lambda x: x[0], lines)))


if __name__ == '__main__':
    try:
        conn = MySQLdb.connect(user='dbuser', passwd='12345678')
        populate_db(conn)

        cursor = conn.cursor()
        while True:
            command = input('> ')
            command = command.strip()
            if command.lower() in ('quit', 'q'):
                break
            if ' ' in command:
                command, name = command.split(' ', maxsplit=1)
            else:
                name = None
            command = command.lower()
            if command in ('line', 'l'):
                if name is not None:
                    print_line_stations(name, cursor)
                else:
                    print('Usage: line LINE_NAME')
            elif command in ('station', 's'):
                if name is not None:
                    print_station_lines(name, cursor)
                else:
                    print('Usage: station STATION_NAME')
            elif command in ('help', 'h', '?'):
                print('Commands: q[uit] | l[ine] LINE_NAME | s[tation] STATION_NAME | h[elp] | ?')
            else:
                print(f'ERROR: `{command}` is not a recognized line or station name.')

    except EOFError:  # Ctrl+D to exit
        pass

    finally:
        conn.query('DROP DATABASE London_Tube;')
