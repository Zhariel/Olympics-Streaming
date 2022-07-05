import csv, os


def generate_path(name: str):
    return os.path.join('..', 'data', 'olympics', name)

def read_athletes():
    athletes = open(generate_path('olympics_athletes.csv'), 'r')
    reader = csv.reader(athletes, delimiter=',')
    for athlete in reader:
        yield ','.join(athlete)

def read_athletes():
    athletes = open(generate_path('olympics_athletes.csv'), 'r')
    reader = csv.reader(athletes, delimiter=',')
    for athlete in reader:
        yield ','.join(athlete)

def read_athletes():
    athletes = open(generate_path('olympics_athletes.csv'), 'r')
    reader = csv.reader(athletes, delimiter=',')
    for athlete in reader:
        yield ','.join(athlete)

def read_athletes():
    athletes = open(generate_path('olympics_athletes.csv'), 'r')
    reader = csv.reader(athletes, delimiter=',')
    for athlete in reader:
        yield ','.join(athlete)