from datetime import datetime
from time import sleep
import logging
import configparser
import irc.bot
import irc.strings
import elasticsearch
import traceback
import argparse
import requests
import random
import multiprocessing

log_level = "INFO"
logger = logging.getLogger(__name__)
logger.setLevel(logging.getLevelName(log_level))
stdout_handler = logging.StreamHandler()
formatter = logging.Formatter('(%(asctime)s) [%(levelname)s]\t%(message)s')
stdout_handler.setFormatter(formatter)
logger.addHandler(stdout_handler)


class CollectBot(irc.bot.SingleServerIRCBot):

    def __init__(self, channel, game, language, server, port, config_file='twitch_collector.conf',
                 verbose=False, log_level="INFO"):
        # Parse Configuration File
        config = configparser.RawConfigParser()
        config.read(config_file)

        # Obtain Configuration Vaules
        nickname = config.get('TwitchSettings', 'nick')
        password = config.get('TwitchSettings', 'password')

        self.language = language
        self.game = game
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
                               body={"nick": nick, "channel": self.channel, "body": body, "timestamp": timestamp,
                                     "game": self.game, "language": self.language})
        except:
            traceback.print_exc()


def setup_index():
    elastic = elasticsearch.Elasticsearch()
    try:
        mapping = {
            "chat_message": {
                "properties": {
                    "nick": {"type": "string", "index": "not_analyzed"},
                    "channel": {"type": "string"},
                    "language": {"type": "string"},
                    "game": {"type": "string"},
                    "body": {"type": "string"},
                    "timestamp": {"type": "date"},
                }
            }
        }

        elastic.indices.create("twitch_chat")
        elastic.indices.put_mapping(index="twitch_chat", doc_type="chat_message", body=mapping)

    except:
        traceback.print_exc()


def get_top_channels(limit=25):
    payload = {'limit': limit}
    url = "https://api.twitch.tv/kraken/streams"
    channels = requests.get(url, params=payload).json()
    return channels


def get_channel_info(channel_name):
    url = "https://api.twitch.tv/kraken/streams/{}".format(channel_name)
    info = requests.get(url).json()
    return info


def collect_top_channels(limit, wait_delay=5):
    collector_threads = {}
    top_chans = get_top_channels(limit=limit)

    for chan in top_chans['streams']:
        chan_name = chan['channel']['name']
        language = chan['channel']['language']
        game = chan['channel']['game']
        logger.info("Starting <{}> ({}) [{}] thread as it is in the top list.".format(language, game, chan_name))
        url = "https://api.twitch.tv/api/channels/{}/chat_properties".format(chan_name)
        chat_servers = requests.get(url).json()['chat_servers']
        target_server = random.choice(chat_servers).split(':')
        collector = CollectBot("#"+chan_name, game, language, target_server[0], int(target_server[1]))
        proc = multiprocessing.Process(target=collector.start)
        collector_threads[chan_name] = proc
        proc.start()

    while True:
        sleep(60*float(wait_delay))
        top_chans = get_top_channels(limit=limit)['streams']
        top_names = set([x['channel']['name'] for x in top_chans])
        current_names = set(collector_threads.keys())

        for name in current_names.difference(top_names):
            # stop the thread for the channels that are in current_names but not in top names
            logger.info("- Killing {} thread as it is no longer in the top list.".format(name))
            collector_threads[name].terminate()
            del(collector_threads[name])

        for chan_name in top_names.difference(current_names):
            # Start threads for channels that are in the top list
            chan_info = get_channel_info(chan_name)
            game = chan_info['stream']['channel']['game']
            language = chan_info['stream']['channel']['language']
            logger.info("Starting <{}> ({}) [{}] thread as it is in the top list.".format(language, game, chan_name))
            url = "https://api.twitch.tv/api/channels/{}/chat_properties".format(chan_name)
            chat_servers = requests.get(url).json()['chat_servers']
            target_server = random.choice(chat_servers).split(':')
            collector = CollectBot("#"+chan_name, game, language, target_server[0], int(target_server[1]))
            proc = multiprocessing.Process(target=collector.start)
            collector_threads[chan_name] = proc
            proc.start()

        current_names = set(collector_threads.keys())
        logger.info("Current channels: {}".format(current_names))


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
        chan_info = get_channel_info(args.channel)
        game = chan_info['stream']['channel']['game']
        language = chan_info['stream']['channel']['language']
        bot = CollectBot("#" + args.channel, game, language, args.server, int(args.port), config_file=args.config,
                         verbose=verbose,)
    elif args.channel and args.server and args.port:
        chan_info = get_channel_info(args.channel)
        game = chan_info['stream']['channel']['game']
        language = chan_info['stream']['channel']['language']
        bot = CollectBot("#" + args.channel, game, language, args.server, int(args.port), verbose=verbose)
    elif args.buildindex:
        print("Building ElasticSearch index...")
        setup_index()
    elif args.top:
        collect_top_channels(args.top, wait_delay=refresh)
    else:
        print(parser.print_help())
