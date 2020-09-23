import threading
from HTML_Fetcher import HTMLFetcher
from HTML_Parser import HTMLParser
import pickle


class WebCrawler:

    def __init__(self, root, num_of_threads, cache_threshold, stop_point=None, filter_domain=None, previous_job=None):
        self.root = root
        if filter_domain:
            self.filter = True
        else:
            self.filter = False
        self.num_of_threads = num_of_threads
        if not previous_job:
            self.output_address_dict = {}
            self.work_queue = [(root,0)]
        else:
            infile = open(previous_job, 'rb')
            self.output_address_dict, self.work_queue = pickle.load(infile)
            infile.close()
        self.parse_counter = 0
        self._output_address_dict_lock = threading.Lock()
        self._work_queue_lock = threading.Lock()
        self._parse_counter_lock = threading.Lock()
        self.now_parsing = []
        self.threads = []
        self.parser = HTMLParser(filter_domain)
        self.queue_not_empty = threading.Event()
        self.queue_not_empty.set()
        self.stop_point = stop_point
        self.cache_threshold = cache_threshold
        self.waiting_threads_counter = 0
        self._waiting_threads_counter_lock = threading.Lock()

    def start_crawler(self):
        for _ in range(self.num_of_threads):
            thread = threading.Thread(target=self.thread_main)
            self.threads.append(thread)
            thread.start()
        for thread in self.threads:
            thread.join()

    def thread_main(self):
        while True:
            self._work_queue_lock.acquire()
            # empty queue - need to wait for more addresses
            if not self.work_queue:
                self.queue_not_empty.clear()
                self._work_queue_lock.release()
                with self._waiting_threads_counter_lock:
                    self.waiting_threads_counter +=1
                    # if all threads are waiting - no more addresses will come and we are done
                    if self.waiting_threads_counter == self.num_of_threads:
                        self.done()
                self.queue_not_empty.wait()
                with self._waiting_threads_counter_lock:
                    self.waiting_threads_counter -=1
                self._work_queue_lock.acquire()
            address = self.work_queue.pop(0)
            self.now_parsing.append(HTMLParser.clean_slash_end(address[0]))
            self._work_queue_lock.release()
            self.parse_address(address)
            # save cache
            with self._parse_counter_lock:
                self.parse_counter += 1
                if self.parse_counter == self.cache_threshold:
                    self.save_cache()
                    self.parse_counter = 0

    def done(self):
        print(self.output_address_dict)
        self.save_cache()
        exit(0)

    def save_cache(self):
        outfile = open('cache', 'wb')
        with self._output_address_dict_lock and self._work_queue_lock:
            pickle.dump((self.output_address_dict, self.work_queue + self.now_parsing), outfile)
        outfile.close()

    def parse_address(self, address_tuple):
        address, dist = address_tuple
        address = HTMLParser.clean_slash_end(address)
        if self.filter and not self.parser.check_filter(address):
            return
        self._output_address_dict_lock.acquire()
        if address in self.output_address_dict:
            self.output_address_dict[address] = min(self.output_address_dict[address], dist)
            self._output_address_dict_lock.release()
        else:
            output_len = None
            self.output_address_dict[address] = dist
            output_len = len(self.output_address_dict)
            self._output_address_dict_lock.release()
            if self.stop_point:
                if output_len >= self.stop_point:
                    self.done()
            print('Parsing {:s}'.format(address))
            print('Already visited {:d} pages'.format(output_len))
            address_list = HTMLParser.parse_text(HTMLFetcher.fetch_from_address(address), address)
            address_tuples_list = []
            for add in address_list:
                address_tuples_list.append((add,dist+1))
            with self._work_queue_lock:
                self.work_queue += address_tuples_list
                print(address)
                self.now_parsing.remove(address)
                if address_list:
                    self.queue_not_empty.set()


crawl = WebCrawler('https://www.greece-islands.co.il',num_of_threads=3,cache_threshold=500,stop_point=1000,filter_domain='https://www.greece-islands.co.il')
# crawl = WebCrawler('https://www.greece-islands.co.il',num_of_threads=3,cache_threshold=500,stop_point=1000)
crawl.start_crawler()
