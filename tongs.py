from urllib2   import urlopen, HTTPError
from urlparse  import urljoin
from threading import Thread, Lock
from argparse  import ArgumentParser
import os, sys, time, re

#------------------------------------------------------------------------------
class Spider(Thread):
    def __init__(self, number, queue, settings):
        self._number      = number
        self._queue       = queue
        self._settings    = settings

        self._is_sleeping = False
        self._sig_term    = False
        Thread.__init__(self)

    def run(self):
        while True:
            if self._sig_term:
                Logger.log("%s terminating" % (self._number))
                return

            url = self._queue.get()
            if not url:
                Logger.log("%s waiting" % (self._number))
                self._is_sleeping += True
                time.sleep(1)
                self._is_sleeping += False
                continue
            Logger.log("%s Fetching %s" % (self._number, url))
            sub_urls = self.fetch_url(url)
            sub_urls = self.filter_suburls(url, sub_urls)
            for u in sub_urls:
                self._queue.put(u)

    def fetch_url(self, url):
        try:
            socket   = urlopen(url)
            document = socket.read()
        except HTTPError, ValueError:
            return []

        sub_urls = re.findall(r'href=[\'"]?([^\'" >]+)', document)
        return sub_urls

    def filter_suburls(self, baseurl, sub_urls):
        filtered = []
        for u in sub_urls:
            if not u.startswith('http://'):
                u = urljoin(baseurl, u)
            if re.match(self.url_regexp, u):
                filtered.append(u)
        return filtered

    def stop(self):
        self.sig_term = True

class UrlsQueue(object):
    def __init__(self):
        self._in_queue  = set()
        self._out_queue = set()

        self._lock = Lock()

    def get(self):
        with self._lock:
            try:
                key = self._in_queue.pop()
            except KeyError:
                return False

            self._out_queue.add(key)
        return key

    def put(self, value):
        if value not in self._out_queue:
            with self._lock:
                self._in_queue.add(value)

    def exists(self, value):
        return value in self._in_queue or value in self._out_queue

class Logger(object):
    """
    Levels:
      0 - just parsed urls
      1 - parsed urls and errors
      2 - parsed urls, errors, threads
    """
    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Singleton, cls).__new__(
                cls, *args, **kwargs)
        return cls._instance

    def __init__(self, loglevel):
        self._loglevel = loglevel

    def log(msg, level=0):
        with Logger.print_lock:
            print msg

#------------------------------------------------------------------------------
def main():
    argparser = ArgumentParser(description='Tongs is a simple tool for grabbing bunch of urls from www-sites')
    argparser.add_argument('-v', '--version', action='version', version='%(prog)s 0.1a')
    argparser.add_argument('url',  help='Initial url')
    argparser.add_argument('-l',   dest='links_regexp',   metavar='LINKS',      help='Process ONLY links that match this regular expression')
    argparser.add_argument('-g',   dest='grab_regexp',    metavar='GRAB',       help='Search links that match this regular expression')
    argparser.add_argument('-t',   dest='threads_count',  metavar='THREADS',    help='Number or simultaneous threads', type=int)
    argparser.add_argument('-ll',  dest='log_level',      metavar='LOGLEVEL',   help='Level of output', type=int, choices=xrange(0, 3))
    argparser.add_argument('-st',  dest='show_timer',     metavar='SHOWTIMER',  help='Show timer after finish', type=bool)
    argparser.set_defaults(
        url           = 'http://fotki.yandex.ru/top/',
        links_regexp  = '^http://fotki.yandex.ru/top/users/.+?/view/\d+$',
        search_regexp = 'http://img-fotki.yandex.ru/get/.+',
        threads_count = 10,
        log_level     = 3,
        show_timer    = True
    )
    if len(sys.argv) == 1:
        argparser.print_help()
        sys.exit()
    else:
        settings = argparser.parse_args()

    queue = UrlsQueue()
    queue.put(settings.url)

    workers = [
        Spider(i, queue, settings)
        for i in range(settings.threads_count)
    ]

    map(lambda w: w.start(), workers)

    timer_start = time.time()
    while any(not w.is_sleeping for w in workers):
        time.sleep(1)
    timer_end = time.time()

    map(lambda w: w.stop(), workers)

    if settings.show_timer:
        print "Finished in %2.1f sec" % (timer_end - timer_start)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print "Interrupted by user"