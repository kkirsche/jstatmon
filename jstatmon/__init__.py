from jstatmon.client import JStatmonClient
from argparse import ArgumentParser


def main():
    parser = ArgumentParser(description='Java statistics monitoring client')
    parser.add_argument(
        '--environment',
        '-n',
        dest='environment',
        nargs='?',
        default='prod',
        help=
        'the environment in which this tool is running (e.g. prod) (optional)')
    parser.add_argument(
        '--verbose',
        '-v',
        dest='verbose',
        action='store_true',
        help='enable verbose logging')
    args = parser.parse_args()

    client = JStatmonClient(verbose=args.verbose, environment=args.environment)
    client.run()
