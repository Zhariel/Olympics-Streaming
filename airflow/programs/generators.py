import csv, json, os


def generate_path(name: str):
    return os.path.join('..', '..', 'data', 'olympics', name)


def encode_data(header, data):
    return json.dumps(dict(zip(header, data))).encode('utf-8')


def read_athletes():
    athletes = open(generate_path('olympic_athletes.csv'), 'r')
    reader = csv.reader(athletes, delimiter=',')
    header = next(reader, None)
    for athlete in reader:
        yield encode_data(header, athlete)


def read_hosts():
    hosts = open(generate_path('olympic_hosts.csv'), 'r')
    reader = csv.reader(hosts, delimiter=',')
    header = next(reader, None)
    for host in reader:
        yield encode_data(header, host)


def read_medals():
    medals = open(generate_path('olympic_medals.csv'), 'r')
    reader = csv.reader(medals, delimiter=',')
    header = next(reader, None)
    for medal in reader:
        yield encode_data(header, medal)


def read_results():
    results = open(generate_path('olympic_results.csv'), 'r')
    reader = csv.reader(results, delimiter=',')
    header = next(reader, None)
    for result in reader:
        yield encode_data(header, result)


athletes = read_athletes()
hosts = read_hosts()
medals = read_medals()
results = read_results()
