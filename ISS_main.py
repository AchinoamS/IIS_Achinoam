import requests
import json
import os
from conf import Config
import db_operation


def get_cities_locations_from_conf_files():
    cities_locations = []
    dir_name = 'cities_locations'
    json_files = get_json_files_names(dir_name)
    for f in json_files:
        with open('{0}/{1}'.format(dir_name, f)) as f:
            data = json.load(f)
            cities_locations.append(data)
    return cities_locations


def get_json_files_names(dir_name):
    path_to_json = '{}/'.format(dir_name)
    json_files = [f_json for f_json in os.listdir(path_to_json) if f_json.endswith('.json')]
    print("json_files: ", json_files)
    return json_files


def get_pass_from_cities_locations(cities_locations):
    all_passes = []
    for city in cities_locations:
        passes = get_pass_for_location(city['lat'], city['lon'], city['name'])
        all_passes.extend(passes)  # todo: check if passes is empty\None
    return all_passes


def get_pass_for_location(lat, lon, city_name):
    params = {
        'lat': lat,
        'lon': lon,
        'n': Config.n_pass_for_api_call
    }
    res = requests.get(url=Config.url, params=params)
    if res.status_code != 200:
        raise ValueError('Cannot get api data. status code: {0}'.format(res.status_code))
    data = res.json()['response']
    data = add_city_to_response_data(data, city_name)
    return data


def add_city_to_response_data(data, city_name):
    for p in data:
        p['city'] = city_name
    return data


def main():
    cities_locations = get_cities_locations_from_conf_files()
    all_passes = get_pass_from_cities_locations(cities_locations)
    db_operation.store_pass_data(all_passes, Config.output_table_name)
    db_operation.update_city_avg_daily_flights()
    db_operation.combine_stats_data_and_export_scv(Config.csv_file_path_for_combine_stats)


if __name__ == "__main__":
    main()
