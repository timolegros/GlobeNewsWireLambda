import requests
from bs4 import BeautifulSoup as soup
import re
from datetime import datetime as dt
from tenacity import retry, stop_after_attempt, wait_fixed
import psycopg2
import datetime
import pytz


class InvalidArticleLink(Exception):
    def __init__(self, link):
        self.link = link
        self.message = f'Something is wrong with this link: {self.link}'
        super().__init__(self.message)


class InvalidArticleText(Exception):
    def __init__(self, article):
        self.article = article
        self.message = f'Something is wrong with the article text: {self.article}'
        super().__init__(self.message)


class Proxies:
    url = 'https://free-proxy-list.net/'

    def __init__(self):
        self.header = {'User-Agent': 'Mozilla/5.0'}
        self.proxyList = []
        self.currentProxy = None
        self.getProxiesDefault()

    def getProxiesDefault(self):
        """
        Makes a request to the free proxy url and parses the resulting html to find all the proxies and their port
        :return: N/A
        """
        try:
            req = requests.get(self.url, headers=self.header)  # sending requests with headers
            url = req.content  # opening and reading the source code
        except requests.exceptions.RequestException as e:
            print('Error getting Proxies')
            raise e
        else:
            pageSoup = soup(url, "html.parser")  # structuring the source code in proper format
            rows = pageSoup.findAll("tr")  # finding all rows in the table if any.
            rows = rows[1:300]  # removes column headers

            self.proxyList = []
            for row in rows:
                cols = row.findAll('td')
                cols = [element.text for element in cols]
                IP = cols[0]  # ipAddress which presents in the first element of cols list
                portNum = cols[1]  # portNum which presents in the second element of cols list
                proxy = IP + ":" + portNum  # concatenating both ip and port
                protocol = cols[6]  # portName variable result will be yes / No
                if protocol == "yes":  # checks if the proxy supports https
                    self.proxyList.append(proxy)

    def refreshProxies(self):
        """
        Refreshes the proxy list. Use this only if you need to manually refresh the list and apply some changes on top
        of the regular list refresh.
        :return: N/A
        """
        self.getProxiesDefault()
        # self.currentProxy = self.proxyList[0]
        # return self.currentProxy

    def getNextProxy(self):
        """
        Gets the next proxy on the list and refreshes the list if it is the last proxy proxy in the list.
        :return: The next proxy available
        """
        if self.currentProxy is None:
            self.currentProxy = self.proxyList[0]
            return self.currentProxy
        elif self.currentProxy != self.proxyList[-1]:
            self.currentProxy = self.proxyList[self.proxyList.index(self.currentProxy) + 1]
            return self.currentProxy
        else:
            self.getProxiesDefault()
            self.currentProxy = self.proxyList[0]
            return self.currentProxy

    def checkProxy(self):
        """
        Checks to see if the current proxy is working by testing a connection to google using the proxy
        :return: the status code or the error that arises
        """
        userAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'
        buildProxy = {'http': self.currentProxy}
        try:
            r = requests.get('https://www.google.com', headers={'User-Agent': userAgent}, proxies=buildProxy, timeout=8)
            return r.status_code
        except (requests.exceptions.Timeout,
                requests.exceptions.ProxyError,
                requests.exceptions.SSLError,
                requests.exceptions.ConnectionError) as e:
            return e


class GNWData:
    """
    The main class used for pulling GlobeNewsWire data.
    """

    # sets the class attribute RSSurl to the RSS feed url that we want to parse data from
    RSSurl = 'https://www.globenewswire.com/Atom/search/srvpLA0OZACn9KBGWOftivocEmgym4Tjxh69n0TAmMM%3d'

    def __init__(self, oldHeadlines):
        """
        Initializes instance variables
        :param oldHeadlines: A list of article headlines that were previously scanned in another GNWData instance -
                             None value if running first scan, no previous data pull was made, or not using this
                             feature because you are checking for duplicates in another manner.
        """
        self.timePulled = None
        self.headlines = []
        self.oldHeadlines = oldHeadlines
        self.entriesList = None

        self.htmlData()
        self.removeOld()

    @classmethod
    def changeRSSurl(cls, link):
        cls.RSSurl = link

    def htmlData(self):
        """
        This function pulls the main data from the RSS link and filters it to return only the data that hasn't already
        been scanned.
        :return:N/A
        """
        try:
            # records data pull time in NAIVE UTC YYYY-MM-DD HH:MM:SS.ffffff format datetime object
            self.timePulled = dt.utcnow()
            content = requests.get(self.RSSurl).text
        except requests.exceptions as e:
            print(e)
            raise
        else:
            pageSoup = soup(content, 'html.parser')
            self.entriesList = pageSoup.find_all('entry')

    def removeOld(self):
        """
        Filters through the pulled entries to find and remove the ones that have already been scanned previously
        :return: N/A
        """
        if self.oldHeadlines is not None:
            validEntries = []
            for item in self.entriesList:
                headline = item.find('title').getText()
                # sets self.headlines to all the valid entry headlines
                if headline not in self.oldHeadlines:
                    self.headlines.append(headline)
                    validEntries.append(item)
            self.entriesList = validEntries

        else:
            # sets self.headlines to all the headlines pulled
            for item in self.entriesList:
                headline = item.find('title').getText()
                self.headlines.append(headline)


class Entry:
    exchanges = ['Nasdaq', 'NASDAQ', 'NYSE', 'OTC', 'Symbol', 'OTCQB', 'OTCPK', 'OTCBB', 'OTC Pink', 'OTC.PK',
                 'OTC PINK', 'OTCMKTS', 'OTCQX', 'OTC BB', 'OTC Markets']

    def __init__(self, entry, proxies):
        """
        Initializes class instance variables
        :param entry: A parsable BeautifulSoup object that contains the info for one article summary/entry
        :param proxies: An instance of the Proxies class
        """
        self.entry = entry
        self.proxies = proxies  # sets the instance variable self.proxies as the Proxies class instance
        self.proxyErrorCounter = 0

        # runs the methods in the correct order on instance creation so that they are accessible using attributes
        self.link = self.getLink()
        self.timeArticleReleased = self.getTimeRelease()
        self.page_soup = self.makeRequest()
        self.article = self.getArticle()
        self.ticker = self.getTicker()
        self.headline = self.getHeadline()

    def getLink(self):
        """
        Parses the entry to find the link to the article
        :return: The link
        """
        self.link = self.entry.find('id').getText()
        return self.link

    def getTimeRelease(self):
        """
        Retrieves the time the article was released from the entry and converts it to a NAIVE datetime object that is
        by already in UTC
        :return: the time the article was released as a NAIVE datetime object in UTC
        """
        self.timeArticleReleased = self.entry.find('updated').getText()
        self.timeArticleReleased = dt.strptime(self.timeArticleReleased, '%Y-%m-%dT%H:%M:%SZ')
        return self.timeArticleReleased

    # if error arises during requests this decorator will retry running the function --- if error persists, the error
    # will be propagated up to the code that called the method in the first place
    @retry(stop=stop_after_attempt(6), wait=wait_fixed(10))
    def makeRequest(self):
        """
        Makes a request to the link to grab the html and parse the results. Sets self.page_soup to the soup object of
        the HTML returned by the request.
        :return: N/A
        """
        try:
            content = requests.get(self.link, proxies={'https': self.proxies.getNextProxy()}).content
        except requests.exceptions.MissingSchema:
            print('this error was raised')
            raise InvalidArticleLink(self.link)
        except requests.exceptions.InvalidSchema:
            print('this error was raised')
            raise InvalidArticleLink(self.link)
        except requests.exceptions.ProxyError:  # catches proxy errors during the request
            print('Proxy Error, trying the next one...')
            self.proxyErrorCounter += 1
            if self.proxyErrorCounter >= 4:
                self.proxies.refreshProxies()  # refreshes the proxy generator
                self.proxyErrorCounter = 0
        except requests.exceptions.RequestException as e:
            print(e)
            raise
        except Exception as e:
            print(e)
        else:
            self.page_soup = soup(content, 'html.parser')
            return self.page_soup

    def getHeadline(self):
        self.headline = self.page_soup.find('h1', {'class': 'article-headline'}).text
        return self.headline

    def getArticle(self):
        """
        Retrieves the article from the link using proxies. Decorator runs the function again a set number of times with
        a time interval between each try incase any errors arise.
        :return: the article text
        """

        self.article = self.page_soup.find('span', {'class': 'article-body'}).text
        return self.article

    def getTicker(self):
        """
        Parses the article text and finds the stock ticker if there is one. InvalidArticleText error is raised if there
        is a problem parsing the text for any reason.
        :return: the stock ticker if there is one and None if there isn't
        """
        try:
            for exchange in self.exchanges:
                if exchange in self.article:
                    exchangeTuple = self.article.partition(exchange)

                    foundCloseBracket = False
                    foundOpenBracket = False

                    for i in range(-1, -35, -1):
                        if exchangeTuple[0][i] == '(':
                            foundOpenBracket = True
                            break
                        elif exchangeTuple[0][i] == ')':
                            foundOpenBracket = False
                            break

                    for i in range(35):
                        if exchangeTuple[2][i] == ')':
                            foundCloseBracket = True
                            break
                        elif exchangeTuple[2][i] == '(':
                            foundCloseBracket = False
                            break

                    if foundOpenBracket and foundCloseBracket:
                        for i in range(30):
                            if exchangeTuple[2][i] == ':':
                                secondString = exchangeTuple[2].partition(':')
                                symbolString = secondString[2].strip()[:5]
                                regex = re.compile('[^;)\s]+')
                                symbol = regex.search(symbolString).group(0)

                                if symbol.isalpha():
                                    self.ticker = symbol
                                    return self.ticker
                                else:
                                    return None
        except TypeError:
            raise InvalidArticleText(self.article)

        except IndexError:
            print(exchangeTuple)


def handler(event=None, context=None):
    lambda_function()
    return


def lambda_function():
    data = GNWData(None)
    proxies = Proxies()

    try:
        connection = psycopg2.connect(user="timolegros",
                                      password="Nashville2020",
                                      host="quadko-paris.ca0fcwommpnv.eu-west-3.rds.amazonaws.com",
                                      port="5432",
                                      database="GlobeNewsWire")

        cursor = connection.cursor()

    except (Exception, psycopg2.Error) as error:
        print(error)
        raise error

    # gets the 10 most recent articles from oldest to newest
    data = data.entriesList[9::-1]
    recordedTickers = []
    for item in data:

        try:
            entry = Entry(item, proxies)
        except Exception:
            continue

        try:
            cursor.execute("""INSERT INTO "Main"("Ticker", "Headline", "PubTime", "Link", "ArticleText")
                                  VALUES (%s, %s, %s, %s, %s);""",
                           (entry.ticker, entry.headline, entry.timeArticleReleased, entry.link, entry.article))
            connection.commit()
            # recordedTickers.append(entry.ticker)
        except Exception as error:
            print(error)

            connection.rollback()

    if connection:
        cursor.close()
        connection.close()

    # try:
    #     connection = psycopg2.connect(user="timolegros",
    #                                   password="Nashville2020",
    #                                   host="quadko-paris.ca0fcwommpnv.eu-west-3.rds.amazonaws.com",
    #                                   port="5432",
    #                                   database="Main")
    #
    #     cursor = connection.cursor()
    #
    # except (Exception, psycopg2.Error) as error:
    #     print(error)
    #     raise error
    #
    # currentTime = datetime.datetime.now(pytz.utc)
    # for item in recordedTickers:
    #     try:
    #         cursor.execute("""INSERT INTO "Tickers"("Ticker", "LastUpdate")
    #                             VALUES (%s, %s);""",
    #                        (item, currentTime))
    #         connection.commit()
    #     except Exception as error:
    #         print(error)
    #         connection.rollback()
    #
    # if connection:
    #     cursor.close()
    #     connection.close()

    # # calls the twitter lambda to update all the tweets for the new news
    # lambda_client = boto3.client("lambda")
    # response = lambda_client.invoke(
    #     FunctionName='TwitterLambda',
    #     InvocationType='Event',
    #     LogType='None',
    #     Payload='Some Data',
    # )


if __name__ == '__main__':
    handler()
