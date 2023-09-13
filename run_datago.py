from fran_branch import CompanyInfoApi as Cia
from juso import JusoXyCsvHandler
import datetime


if __name__ == '__main__':
    d_num = datetime.datetime.now().strftime('%m%d_%H%M')

    c = Cia(d_num)
    c.download()
    c.get_updated('0628_1954', d_num)
    updated_file_name = c.b.get_file_name(d_num, '_update')

    js = JusoXyCsvHandler()
    js.add_to_csv()



