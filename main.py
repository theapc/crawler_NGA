from nga_crawal import Nga_Crawal
from multiprocessing import Pool
import sys

cookies = {
    'ngaPassportCid': 'Z8f2qd485q9j66bp1996a81s50hbrg4ash7d0nrn',
    'ngaPassportUid': '347702',
    }
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko)'
                         ' Chrome/36.0.1985.125 Safari/537.36', 'X-Requested-With': 'XMLHttpRequest',
           }

crawler = Nga_Crawal(cookies, headers, '-7')

if __name__ == '__main__':
    old_stdout = sys.stdout
    log_file = open("message.log", "w")
    sys.stdout = log_file
    sys.stdout = old_stdout
    p = Pool(16)  # Pool tells how many at a time
    records = p.map(crawler.append_tid_db, range(500))
    p.terminate()
    p.join()
    urls = crawler.gen_tid_url_lists()
    p = Pool(16)  # Pool tells how many at a time
    posts = p.map(crawler.crawl_post_page, urls)
    p.terminate()
    p.join()
    crawler.dedupe_pid()
    crawler.dedupe_tid()
    log_file.close()
    #finished

