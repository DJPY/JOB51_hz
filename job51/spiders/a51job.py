# -*- coding: utf-8 -*-


from scrapy import Request

'''
class A51jobSpider(scrapy.Spider):
    name = '51job'
    allowed_domains = ['51job.com']
    start_urls = ['http://51job.com/']

    def parse(self, response):
        pass
'''

from scrapy_redis.spiders import RedisSpider
#import json

from DjSpider import DBcrud as db
#from DjSpider import Rabbitmq

class A51jobSpider(RedisSpider):
    name = '51job'
    redis_key = '51job:start_urls'
    '''
    def __init__(self, *args, **kwargs):

        # Dynamically define the allowed domains list.
        domain = kwargs.pop('domain', '')
        self.allowed_domains = filter(None, domain.split(','))
        super(A51jobSpider, self).__init__(*args, **kwargs)
    '''
#SADD 51job:start_urls

    def __init__(self):
        super(A51jobSpider, self).__init__()
        #self.redisCli = db.Redis_crud()
        self.mongoCli = db.Mongo_crud()
        #self.rabbitCli = Rabbitmq.Rabbitmq('51job_hz_company')

    def parse(self, response):
        print('====================================')
        print(response.url)
        for i in range(5):
            url = 'http://search.51job.com/list/080200,000000,0000,00,9,99,%%2B,2,%d.html?lang=c&stype=1&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&lonlat=0%%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=&dibiaoid=0&address=&line=&specialarea=00&from=&welfare=' % i
            yield Request(url, callback=self.commpanyURL)

    def commpanyURL(self, response):
        companyUrl = response.xpath('//span[@class="t2"]/a/@href').extract()
        companyName = response.xpath('//span[@class="t2"]/a/@title').extract()
        # rabb.product()
        for i in range(len(companyName)):
            # print(companyName[i].encode().decode('gbk'))
            cn = companyName[i]
            cu = companyUrl[i]
            data = {}
            data['companyUrl'] = cu
            data['companyName'] = cn
            #print(data)
            yield Request(cu, callback=self.jobs, meta=data)

    def jobs(self, response):
        #print('$$$$$$$$$$$$$$$$$$$$$$$$')
        jobLinks = response.xpath('//*[@id="joblistdata"]/div/p/a/@href').extract()
        data = {}
        data['companyName'] =response.meta['companyName']
        data['companyUrl'] = response.meta['companyUrl']
        for i in jobLinks:
            #print('***************')
            #print(i)
            data['jobLink'] = i
            yield Request(url=i, callback=self.jobInfo, meta=data)

    def jobInfo(self, response):
        #print('&&&&&&&&&&&&&&&&&&&&&&&&&&')
        data = {}
        data['companyName'] = response.meta['companyName']
        data['companyUrl'] = response.meta['companyUrl']
        data['jobLink'] = response.meta['jobLink']
        try:
            salary = response.xpath('/html/body/div[3]/div[2]/div[2]/div/div[1]/strong/text()').extract()[0]
        except:
            salary='None'
        company = response.xpath('/html/body/div[3]/div[2]/div[2]/div/div[1]/p[2]/text()').extract()[0]
        companyType = company.replace('\r','').replace('\t','').replace('\xa0','').replace('\n','').replace(' ','')
        area = response.xpath('/html/body/div[3]/div[2]/div[2]/div/div[1]/span/text()').extract()[0]
        recruitInfo = response.xpath('/html/body/div[3]/div[2]/div[3]/div[1]/div/div/span/text()').extract()
        jobInfo = response.xpath('//div[@class="bmsg job_msg inbox"]/p/text()').extract()
        data['salary'] = salary
        data['companyType'] = companyType
        data['area'] = area
        data['recruitInfo'] = recruitInfo
        data['jobInfo'] = jobInfo
        #print(data)
        #self.rabbitCli.producer('direct_51job_hz','direct','51job_hz_company',data)
        self.mongoCli.mongo_insert_data('51job_hz_job', data, '51job_hz_company')

