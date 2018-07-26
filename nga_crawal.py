from bs4 import BeautifulSoup as BS


class Nga_Crawal(object):
    import time
    timestr = time.strftime("%Y%m%d-%H%M")
    import thulac
    thu1 = thulac.thulac(user_dict='S:\\python\\NGA\\emj.txt', seg_only=True)

    def __init__(self, cookies, headers, fid):
        self.cookies = cookies
        self.headers = headers
        self.fid = fid

    def gen_page_url(self, pages):
        url = 'https://bbs.ngacn.cc/thread.php?fid={}&page={}&lite=xml'.format(self.fid, pages)
        return url

    def gen_post_url(self,tid,page):
        url = "https://bbs.ngacn.cc/read.php?tid={}&page={}&lite=xml&v2".format(tid,page)
        return url

    def safe_get(self, url):
        import time
        import requests
        html = ''
        while html == '':
            try:
                html = requests.get(url, cookies=self.cookies, headers=self.headers)
                break
            except:
                print("Connection refused by the server..")
                print("Let me sleep for 2 seconds")
                print("ZZzzzz...")
                time.sleep(2)
                print("Was a nice sleep, now let me continue...")
                continue
        return html

    def format_html_att(self, reply):
        d = {}
        for x in reply.findChildren(recursive=False):
            d[x.name] = x.text
        return d

    def crawl_tid_page(self, n_page):
        # this function is to crawl a forum page and return tid list
        from bs4 import BeautifulSoup as BS
        url_page = self.gen_page_url(n_page)
        response = self.safe_get(url_page)
        soup = BS(response.text, 'lxml')
        tids = soup.find('__t').findAll('item', recursive=False)
        tids = [self.format_html_att(x) for x in tids]
        return tids

    def clean_comment(self, x):
        import re
        try:
            x = re.sub('\[quote].*\[/quote\]', '', x, flags=re.DOTALL)
            x = re.sub('\[b\].*\[/b\]', '', x, flags=re.DOTALL)
            x = re.sub('\[img\].*\[/img\]', '', x, flags=re.DOTALL)
            x = re.sub('\[flash\].*\[/flash\]', '', x, flags=re.DOTALL)
            x = re.sub('\[url.*\[/url\]', '', x, flags=re.DOTALL)
            x = re.sub('\<br/>.*\<br/\>', '', x, flags=re.DOTALL)
            x = re.sub('\<br/>', '', x, flags=re.DOTALL)
            x = re.sub('\<br/\>', '', x, flags=re.DOTALL)
            x = re.sub('\[del]', '', x, flags=re.DOTALL)
            x = re.sub('\[\/del]', '', x, flags=re.DOTALL)
            x = re.sub('\[\w*=.*?\]', '', x, flags=re.DOTALL)
            x = re.sub('\[(\/|@).*\]', '', x, flags=re.DOTALL)
            x = re.sub(
                '(\[\/quote\])|(\[\/list\])|(\[\/align\])|(\[\/code\])|(\[\/collapse.*?\])|(\[\/size.*?\])|(\[\/read\.php.*?\])|(\[\/color.*?\])|(\[\/url.*?\])',
                '', x, flags=re.DOTALL)
        except:
            x = x
        return x

    def crawl_post_page(self, url_page):
        from pymongo import MongoClient
        client = MongoClient()
        db = client.NGA_multi
        from bs4 import BeautifulSoup as BS
        # this function is to crawl a post page and return reply list
        response = self.safe_get(url_page)
        soup = BS(response.text, 'lxml')
        if soup.find('__message'):
            return None
        else:
            reply = soup.find('__r').findAll('item', recursive=False)
            reply = [self.format_html_att(x) for x in reply]
            for lou in reply:
                try:
                    lou['content'] = self.thu1.cut(self.clean_comment(lou['content']), text=True)
                    lou['subject'] = self.thu1.cut(self.clean_comment(lou['subject']), text=True)
                except Exception as e:
                    print(url_page + ' ' + e)
                    pass
            db.NGA_replies.insert_many(reply)

    def append_tid_db(self, n):
        from pymongo import MongoClient
        client = MongoClient()
        db = client.NGA_multi
        tids = self.crawl_tid_page(n)
        for _newpost in tids:
            _post = db.tid.find_one({'tid': _newpost['tid']})  # get old information from tid table for new post
            if not _post:  # if post is in tid table
                db.tid.insert_one(_newpost)
                print("tid {} inserted".format(_newpost['tid']) + '\n')
            elif _post and (int(_newpost['replies']) > int(_post['replies'])):  # if post have more replies
                print("tid {} updated into {} replies".format(_newpost['tid'], _newpost['replies']) + '\n')
                db.tid.update_one({'tid': _post['tid']},
                                  {"$set": {'replies': _newpost['replies']}})  # update reply count to new info
            else:
                print('tid {} no need to update'.format(_newpost['tid']) + '\n')

    def dedupe_tid(self):
        from pymongo import MongoClient
        from pymongo import DeleteOne
        client = MongoClient()
        db = client.NGA_multi
        cursor = db.tid.aggregate(
            [
                {"$group": {"_id": "$tid", "unique_ids": {"$addToSet": "$_id"}, "count": {"$sum": 1}}},
                {"$match": {"count": {"$gte": 2}}}
            ]
        )
        response = []
        for doc in cursor:
            del doc["unique_ids"][0]
            for id in doc["unique_ids"]:
                response.append(DeleteOne({'_id': id}))
        if len(response) >= 1:
            db.tid.bulk_write(response)
        cursor.close()


    def dedupe_pid(self):
        from pymongo import MongoClient
        from pymongo import DeleteOne
        client = MongoClient()
        db = client.NGA_multi
        cursor = db.NGA_replies.aggregate(
            [
                {"$group": {"_id": {"tid": "$tid", "pid": "$pid"}, "unique_ids": {"$addToSet": "$_id"},
                            "count": {"$sum": 1}}},
                {"$match": {"count": {"$gte": 2}}}
            ], allowDiskUse=True
        )
        response = []
        for doc in cursor:
            del doc["unique_ids"][0]
            for id in doc["unique_ids"]:
                response.append(DeleteOne({'_id': id}))
        if len(response) >= 1:
            db.NGA_replies.bulk_write(response)
        cursor.close()

    def gen_tid_url_lists(self):
        from pymongo import MongoClient
        import pandas as pd
        client = MongoClient()
        db = client.NGA_multi
        crawl = db.NGA_replies.aggregate(
            [
                {"$group": {"_id": "$tid", "count": {"$sum": 1}}}
            ]
        )
        exist_tid = pd.DataFrame(list(crawl))
        last_date = str(int(self.time.time()) - 3600 * 1400)
        cursor1 = db.tid.find({'postdate': {'$gte': last_date}})
        urls = []
        for record in cursor1:
            try:
                _id = record['tid']
                if _id not in list(exist_tid['_id']):
                    for page in range(1, int(int(record['replies']) / 20) + 2):
                        try:
                            urls.append("https://bbs.ngacn.cc/read.php?tid={}&page={}&lite=xml&v2".format(_id, page))
                        except Exception as e:
                            print(str(e) + '\n')
                            pass
                elif (int(exist_tid[exist_tid['_id'] == _id]['count']) < int(record['replies'])):
                    for page in range(int(int(exist_tid[exist_tid['_id'] == _id]['count']) / 20) + 1,
                                      int(int(record['replies']) / 20) + 2):
                        try:
                            urls.append("https://bbs.ngacn.cc/read.php?tid={}&page={}&lite=xml&v2".format(_id, page))
                        except Exception as e:
                            print(str(e) + '\n')
                            pass
                else:
                    print(_id + ' no new replies' + '\n')
            except Exception as e:
                print(_id + e + '\n')
                pass
        cursor1.close()
        return urls
