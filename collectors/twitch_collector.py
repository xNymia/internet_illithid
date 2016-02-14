# TODO: Verify working example code
# TODO: Add additional logic for picking proper twitch chat server via Twitch REST API
# TODO: Move configuration of collector over to command line options instead of config file for account information
import configparser
import sys
import irc.bot, irc.strings
import elasticsearch
import traceback
from datetime import datetime


class CollectBot(irc.bot.SingleServerIRCBot):

    def __init__(self, channel, config_file='conf/twitch_collector.conf'):
        # Parse Configuration File
        config = configparser.RawConfigParser()
        config.read(config_file)

        # Obtain Configuration Vaules
        server = config.get('TwitchSettings', 'server')
        port = int(config.get('TwitchSettings', 'port'))
        nickname = config.get('TwitchSettings', 'nick')
        username = config.get('TwitchSettings', 'username')
        self.password = config.get('TwitchSettings', 'password')
        self.channel = channel

        irc.bot.SingleServerIRCBot.__init__(self, [(server, port, self.password)], nickname, nickname)

    def on_welcome(self, c, e):
        c.join(self.channel)

    def on_pubmsg(self, c, e):
        print(e.source.split('!')[0], e.arguments[0])
        self.index_message([e.source.split('!')[0], e.arguments[0]])
        return

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
    channel = sys.argv[1]
    bot = CollectBot(channel)
    bot.start()