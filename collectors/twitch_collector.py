# TODO: Add additional logic for picking proper twitch chat server via Twitch REST API
# TODO: Implement logic for collecting from the top n streamers based on viewer count
# TODO: Option to connect to most popular channels on startup.
import configparser
import irc.bot
import irc.strings
import elasticsearch
import traceback
import argparse
from datetime import datetime


class CollectBot(irc.bot.SingleServerIRCBot):

    def __init__(self, channel, server, port, config_file='twitch_collector.conf', verbose=False):
        # Parse Configuration File
        config = configparser.RawConfigParser()
        config.read(config_file)

        # Obtain Configuration Vaules
        nickname = config.get('TwitchSettings', 'nick')
        password = config.get('TwitchSettings', 'password')
        self.channel = channel
        self.verbose = verbose

        irc.bot.SingleServerIRCBot.__init__(self, [(server, port, password)], nickname, nickname)

    def on_welcome(self, c, e):
        c.join(self.channel)

    def on_pubmsg(self, c, e):
        if self.verbose:
            print(e.source.split('!')[0], e.arguments[0])
        self.index_message([e.source.split('!')[0], e.arguments[0]])

    def setup_index(self):
        elastic = elasticsearch.Elasticsearch()
        try:
            mapping = {
                "chat_message": {
                    "properties": {
                        "nick": {"type": "string", "index": "not_analyzed"},
                        "channel": {"type": "string"},
                        "body": {"type": "string"},
                        "timestamp": {"type": "date"},
                    }
                }
            }

            elastic.indices.create("twitch_chat")
            elastic.indices.put_mapping(index="twitch_chat", doc_type="chat_message", body=mapping)

        except:
            traceback.print_exc()

    def index_message(self, message):
        elastic = elasticsearch.Elasticsearch()

        try:
            nick = message[0]
            body = message[1]
            timestamp = datetime.utcnow()

            elastic.index(index='twitch_chat', doc_type='chat_message', body={"nick": nick,
                                                                           "channel": self.channel, "body": body,
                                                                           "timestamp": timestamp})
        except:
            traceback.print_exc()


if __name__ == "__main__":

    # argparse configuration
    parser = argparse.ArgumentParser()
    parser.add_argument("--buildindex", action="store_true")
    parser.add_argument("--config")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("channel")
    parser.add_argument("server")
    parser.add_argument("port")
    args = parser.parse_args()

    verbose = args.verbose

    if args.config:
        bot = CollectBot("#" + args.channel, args.server, int(args.port), config_file=args.config, verbose=verbose)
    else:
        bot = CollectBot("#" + args.channel, args.server, int(args.port), verbose=verbose)

    if args.buildindex:
        print("Building ElasticSearch index...")
        bot.setup_index()
    else:
        bot.start()
