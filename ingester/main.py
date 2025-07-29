
import argparse
from cli.folder_listener import listen
from cli.trade_ingester import ingest_periodic

def main():
    parser = argparse.ArgumentParser(description="Ingester CLI")
    subparsers = parser.add_subparsers(dest="command")

    # Listener command
    listen_parser = subparsers.add_parser("listen", help="Listen for new files in a directory")
    listen_parser.add_argument("path", help="The directory path to listen to")

    # Ingester command
    ingest_parser = subparsers.add_parser("ingest", help="Periodically ingest a trade log file")
    ingest_parser.add_argument("file", help="The file to ingest periodically")

    args = parser.parse_args()

    if args.command == "listen":
        listen(args.path)
    elif args.command == "ingest":
        ingest_periodic(args.file)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
