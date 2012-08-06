from urllib2   import urlopen, HTTPError, URLError
from urlparse  import urlparse, urljoin, urlsplit, urlunsplit
from threading import Thread, Lock, Event
from argparse  import ArgumentParser
from logging   import log
import os, sys, time, re, logging

#------------------------------------------------------------------------------
class Spider(Thread):
    def __init__(self, number, queue, settings):
        Thread.__init__(self)
        self._sig_term    = Event()
        self._is_sleeping = False
        self.daemon       = True

        self._number      = number
        self._queue       = queue
        self._settings    = settings

    @property
    def is_sleeping(self):
        return self._is_sleeping

    def run(self):
        while True:
            if self._sig_term.isSet():
                log(0, "%s terminating", self._number)
                return

            url = self._queue.get()
            if not url:
                log(0, "%s waiting", self._number)
                self._is_sleeping += True
                time.sleep(1)
                self._is_sleeping += False
                continue
            log(10, "%s Fetching %s", self._number, url)
            sub_urls = self._fetch_url(url)
            sub_urls = self._filter_suburls(url, sub_urls)
            for u in sub_urls:
                self._queue.put(u)

    def _fetch_url(self, url):
        try:
            socket   = urlopen(url)
            document = socket.read()
        except (HTTPError, URLError, ValueError):
            log(20, '%s Error: incorrect url %s', self._number, url)
            return []

        sub_urls = re.findall(r'href=[\'"]?([^\'" >]+)', document)

        if self._settings.grab_regexp:
            for m in re.findall(self._settings.grab_regexp, document):
                log(50, m)

        return sub_urls

    def _filter_suburls(self, baseurl, sub_urls):
        filtered = []
        for u in sub_urls:
            if not u.startswith('http://'):
                u = urljoin(baseurl, u)
            if re.match(self._settings.links_regexp, u):
                filtered.append(u)
        return filtered

    def stop(self):
        self._sig_term.set()

#------------------------------------------------------------------------------
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

#------------------------------------------------------------------------------
def main():
    argparser = ArgumentParser(description='Tongs is a simple console tool for www-site traversal and grabbing bunch of urls')
    argparser.add_argument('-v', '--version', action='version', version='%(prog)s 0.1a')
    argparser.add_argument('url',  help='Initial url')
    argparser.add_argument('-l',   dest='links_regexp',   metavar='LINKS',      help='Process ONLY links that match this regular expression')
    argparser.add_argument('-g',   dest='grab_regexp',    metavar='GRAB',       help='Search links that match this regular expression')
    argparser.add_argument('-a',   dest='amount',         metavar='AMOUNT',     help='Stop after grabbing N links', type=bool)
    argparser.add_argument('-t',   dest='threads_count',  metavar='THREADS',    help='Number or simultaneous threads', type=int)
    argparser.add_argument('-ll',  dest='log_level',      metavar='LOGLEVEL',   help='Level of output', type=int, choices=xrange(0, 51))
    argparser.add_argument('-st',  dest='show_timer',     metavar='SHOWTIMER',  help='Show timer after finish', type=bool)
    argparser.set_defaults(
        threads_count = 10,
        log_level     = 0,
        show_timer    = True
    )
    if len(sys.argv) == 1:
        argparser.print_help()
        sys.exit()
    else:
        settings = argparser.parse_args()

    logging.basicConfig(level = settings.log_level, format = '%(message)s')

    if not settings.links_regexp:
        try:
            u = urlsplit(settings.url)
        except URLError:
            raise Exception('Incorrect url')
        settings.links_regexp = urlunsplit(list(u)[:3] + ['','']) + '.*'
        log(10, 'Looking in %s', settings.links_regexp)

    queue = UrlsQueue()
    queue.put(settings.url)

    workers = [
        Spider(i, queue, settings)
        for i in range(settings.threads_count)
    ]

    map(lambda w: w.start(), workers)

    timer_start = time.time()
    try:
        while any(not w.is_sleeping for w in workers):
            time.sleep(1)
    except KeyboardInterrupt:
        exit(0)
    timer_end = time.time()

    map(lambda w: w.stop(), workers) #Send the termination signal
    map(lambda w: w.join(), workers) #And wait for all threads to stop

    if settings.show_timer:
        log(50, "Finished in %2.1f sec", timer_end - timer_start)

#------------------------------------------------------------------------------
if __name__ == '__main__':
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        print "Interrupted by user"
        exit(0)