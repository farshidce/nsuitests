import time
from selenium import webdriver
from common.logger import Logger

log = Logger.get_logger()

class Verifier(object):

    navigator = None
    def __init__(self,navigator):
        self.navigator = navigator

    def verify_sect_buckets(self,expected_buckets):
        #we need to verify to make sure this table
        #shows all the bucket
        self.navigator.managed_buckets()
        names = PageParser(self.navigator).bucket_names()
        time.sleep(5)
        log.info('buckets shown in the ui {0}'.format(names))
        log.info('buckets retrieved from rest {0}'.format(expected_buckets))
        for expected_bucket in expected_buckets:
            if not expected_bucket in names:
                return False
        return True

class PageParser(object):
    navigator = None
    def __init__(self,navigator):
        self.navigator = navigator

    def bucket_names(self):
        names = []
        self.navigator.managed_buckets()
        # abc= d.find_element_by_xpath('//td[@class="bucket_name"]')
        td_buckets = self.navigator.browser.find_elements_by_xpath('//td[@class="bucket_name"]')
        for td_bucket in td_buckets:
            names.append(td_bucket.text)
#            a_link = td_bucket.find_element_by_tag_name('a')
        return names
            


class Navigator(object):

    ip = ''
    base = ''
    browser = None

    def __init__(self,ip):
        self.ip = ip
        self.base = 'http://{0}:8091'.format(self.ip)
        self.browser = webdriver.Firefox()

    def _go_to_login(self):
        url = '{0}/index.html'.format(self.base)
        self.browser.get(url)
        pass

    def managed_buckets(self):
        #let's make sure we are logged in
        url = '{0}/index.html#sec=buckets'.format(self.base)
        self.browser.get(url)

    def login(self,
              username = 'Administrator',
              password = 'password'):
        url = '{0}/index.html'.format(self.base)
        self.browser.get(url)
        elem_password = self.browser.find_element_by_id('password2_inp')
        elem_username = self.browser.find_element_by_id('login_inp')
        elem_password.clear()
        elem_username.clear()
        elem_username.send_keys(username)
        elem_password.send_keys(password)
        btn_signin = self.browser.find_element_by_xpath('//input[@value="Sign In"]')
        if btn_signin:
            btn_signin.submit()
            log.info(self.browser.title)
            time.sleep(10)
            return True
        return False


        #type u,p in the page and click submit

    def close(self):
        if self.browser:
            log.info('closing the browser...')
            self.browser.close()
            self.browser = None

