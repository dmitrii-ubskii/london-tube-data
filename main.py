#!/usr/bin/env python3

import json

import MySQLdb


if __name__ == '__main__':
    with open('train-network.json') as f:
        data = json.load(f)

    try:
        conn = MySQLdb.connect(user='dbuser', passwd='12345678')
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

    finally:
        conn.query('DROP DATABASE London_Tube;')
