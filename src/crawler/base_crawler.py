from requests import request
import re
from src.enum.http_enum import HttpMethods, HttpResponses
from src.enum.encoding_enum import EncodingEnum

class BaseCrawler:
    def __init__(self, encoding=EncodingEnum.UTF_8, timeout=20):
        if not isinstance(encoding, EncodingEnum):
            raise TypeError('encoding has to be EncodingEnum')
        self.encoding = encoding
        self.timeout = timeout

    def get_response(self, url, method=HttpMethods.GET, params={}, headers=None, is_secure=True):
        matched_result = re.match(r"^https?:\/\/(.+)", url)
        protocol = 'https' if is_secure else 'http'
        if matched_result:
            # プロトコルを含んでいる場合プロトコルを除く
            url = matched_result.group(1)
        url = protocol + '://' + url
        try:
            res = request(method.value, url, params=params, headers=headers, timeout=self.timeout)
        except Exception as e:
            print(e)
            res = {'status' : HttpResponses.INTERNER_SERVER_ERROR.value}
        return res

    def run(self):
        pass
