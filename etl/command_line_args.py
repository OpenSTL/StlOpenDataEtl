import argparse

def getCommandLineArgs(local_source=True, db=True):
    parser = argparse.ArgumentParser()
    if db:
        parser.add_argument('--db', nargs='?', type=str, choices=['dev','prod'], default='dev', help='dev: use local database; prod: use production database')
    if local_source:
        parser.add_argument('--local-sources', nargs='+', type=str, help='local data files to use in place of internet sources.')
    args = parser.parse_args()
    return args
