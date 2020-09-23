import re

regex_pattern = '(?<=\<a href=")(.*?)(?=\")'


class HTMLParser:

    def __init__(self,filter):
        self.filter = filter

    @staticmethod
    def parse_text(text, root_address):
        refs = re.findall(regex_pattern, text)
        # remove '#'
        refs = list(filter(lambda a: a != '#', refs))
        # fix relative addresses
        fixed_refs = []
        for address in refs:
            new_address = address
            if root_address.endswith(new_address):
                continue
            if new_address[0] == '/':
                prefix = new_address.split('/')[1]
                join_index = root_address.find(prefix)
                if join_index == -1:
                    new_address = root_address+new_address
                else:
                    new_address = HTMLParser.clean_slash_end(root_address[:join_index])+new_address
            fixed_refs.append(new_address)

        return fixed_refs

    def check_filter(self,address):
        return address.startswith(self.filter)

    @staticmethod
    def clean_slash_end(string):
        if string.endswith('/'):
            return string[:-1]
        return string
