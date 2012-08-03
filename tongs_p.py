from multiprocessing import Process, Lock, Event, Value
from urllib2 import urlopen, HTTPError
from urlparse import urljoin
from multiprocessing.managers import BaseManager
from collections import OrderedDict
import os, sys, time, re

class Spider(Process):
    def __init__(self, number, queue, wait_count, term_event, url_regexp):
        self.number     = number
        self.queue      = queue
        self.url_regexp = url_regexp
        self.wait_count = wait_count
        self.term_event = term_event
        Process.__init__(self)

    def run(self):
        while True:
            if self.term_event.is_set():
                print "%s terminating" % (self.number)
                sys.exit(0)

            url = self.queue.get()
            if not url:
                print "%s waiting" % (self.number)
                self.wait_count.value += 1
                time.sleep(2)
                self.wait_count.value -= 1
                continue
            print "%s Fetching %s" % (self.number, url)
            sub_urls = self.fetch_url(url)
            sub_urls = self.filter_suburls(url, sub_urls)
            for u in sub_urls:
                self.queue.put(u)

    def fetch_url(self, url):
        try:
            socket = urlopen(url)
            document = socket.read()
        except HTTPError:
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

class UrlsQueue(object):
    in_queue  = {}
    out_queue = {}

    def get(self):
        try:
            (key, val) = self.in_queue.popitem()
        except KeyError:
            return False

        self.out_queue[key] = 0
        return key

    def put(self, value):
        if not self.out_queue.has_key(value):
            self.in_queue[value] = 0

    def exists(self, value):
        return self.in_queue.has_key(value) or \
            self.out_queue.has_key(value)

class QueueManager(BaseManager): pass
QueueManager.register('UrlsQueue', UrlsQueue)

if __name__ == '__main__':
    print "Tongs 0.1a"

    manager = QueueManager()
    manager.start()
    queue = manager.UrlsQueue()
    wait_count = Value('i', 0)
    queue.put('http://docs.python.org/library/constants.html')
    url_regexp = r'^http://docs.python.org/library/constants.html.*'
    sig_term = Event()

    workers = [
        Spider(i, queue, wait_count, sig_term, url_regexp)
        for i in range(10)
    ]

    for w in workers:
        w.start()

    while wait_count.value < 10:
        time.sleep(1)

    print "Sending sig term"
    sig_term.set()