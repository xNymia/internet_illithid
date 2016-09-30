import irc.bot
import irc.strings
import elasticsearch
import configparser
import traceback
import argparse
import logging
from datetime import datetime

log_level = "INFO"
logger = logging.getLogger(__name__)
logger.setLevel(logging.getLevelName(log_level))
stdout_handler = logging.StreamHandler()
formatter = logging.Formatter('(%(asctime)s) [%(levelname)s]\t%(message)s')
stdout_handler.setFormatter(formatter)
logger.addHandler(stdout_handler)


class CollectBot(irc.bot.SingleServerIRCBot):

    def __init__(self, channel, server, port, config_file='twitch_collector.conf', verbose=False, log_level="INFO"):
        # Parse Configuration File
        config = configparser.RawConfigParser()
        config.read(config_file)

        # Obtain Configuration Vaules
        nickname = config.get('TwitchSettings', 'nick')
        password = config.get('TwitchSettings', 'password')

        self.channel = channel
        self.verbose = verbose
        self.elastic = elasticsearch.Elasticsearch()

        logger.setLevel(logging.getLevelName(log_level))

        irc.bot.SingleServerIRCBot.__init__(self, [(server, port, password)], nickname, nickname)

    def on_welcome(self, c, e):
        c.join(self.channel)

    def on_pubmsg(self, c, e):
        if self.verbose:
            print(e.source.split('!')[0], e.arguments[0])
        self.index_message([e.source.split('!')[0], e.arguments[0]])

    def index_message(self, message):
        logger.debug("Indexing: [{}] <{}> {}".format(self.channel, message[0], message[1]))

        try:
            nick = message[0]
            body = message[1]
            timestamp = datetime.utcnow()

            self.elastic.index(index='twitch_chat', doc_type='chat_message',
                               body={"nick": nick, "channel": self.channel, "body": body, "timestamp": timestamp})
        except:
            traceback.print_exc()


def setup_index():
    elastic = elasticsearch.Elasticsearch()
    try:
        mapping = {
            "chat_message": {
                "properties": {
                    "nick": {"type": "string", "index": "not_analyzed"},
                    "body": {"type": "string"},
                    "channel": {"type": "string"},
                    "timestamp": {"type": "date"},
                }
            }
        }

        elastic.indices.create("twitch_chat")
        elastic.indices.put_mapping(index="twitch_chat", doc_type="chat_message", body=mapping)

    except:
        traceback.print_exc()

if __name__ == "__main__":

    # argparse configuration
    parser = argparse.ArgumentParser()
    parser.add_argument("--buildindex", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--config")
    parser.add_argument("--log")
    parser.add_argument("--channel", "-c")
    parser.add_argument("--server", "-s")
    parser.add_argument("--port", "-p")
    parser.add_argument("--refresh", "-r")
    parser.add_argument("--top", help="Collect from the top n streams.")
    args = parser.parse_args()

    # Configure logging
    if args.log:
        log_level = args.log
        logger.setLevel(logging.getLevelName(log_level))

    verbose = args.verbose

    if args.refresh:
        refresh = args.refresh
    else:
        refresh = 5

    if args.config and args.channel and args.server and args.port:
        bot = CollectBot("#" + args.channel, args.server, int(args.port), config_file=args.config, verbose=verbose,)
        bot.start()
    elif args.channel and args.server and args.port:
        bot = CollectBot("#" + args.channel, args.server, int(args.port))
        bot.start()
    elif args.buildindex:
        print("Building ElasticSearch index...")
        setup_index()
    else:
        print(parser.print_help())
