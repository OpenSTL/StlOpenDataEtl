import argparse

def getCommandLineArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('--local-sources', nargs='+', type=str, help='local data files to use in place of internet sources.')
    args = parser.parse_args()
    return args