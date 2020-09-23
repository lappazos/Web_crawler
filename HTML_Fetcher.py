import urllib.request


class HTMLFetcher:

    @staticmethod
    def fetch_from_address(address):
        try:
            fp = urllib.request.urlopen(address)
        except Exception:
            print('ERROR - '+address)
            return ''
        data = fp.read()
        html = data.decode("utf8")
        fp.close()
        return html
