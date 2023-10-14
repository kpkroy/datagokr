import re


class JusoProcess:
    def __init__(self):
        self.name_hd = ["서울특별시", "부산광역시", "대구광역시", "인천광역시", "광주광역시", "대전광역시", "울산광역시",
                        "세종특별자치시", "경기도", "충청북도", "충청남도", "전라북도", "전라남도", "경상북도", "경상남도",
                        "제주특별자치도", "강원특별자치도"]

    def has_issue(self, addr):
        splitted = [x for x in addr.split(" ") if x]
        if not splitted:
            return True
        if splitted[0] not in self.name_hd:
            return True
        return False

    def cleanse(self, addr):
        if not self.has_issue(addr):
            return addr


    def get_b_code(self):
        pass