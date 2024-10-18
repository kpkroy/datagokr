from tabulate import tabulate


def run_cli_bohum():
    while True:
        ask_d = input('\n!  고용보험 데이터!'
                      '\n  1. page number'
                      '\n  2. 전체'
                      '\n  > ')
        if ask_d == '1':
            page_num = input('\n!  page num? > ')
            page_num = 1 if page_num == '' else int(page_num)
            df = b.download_a_page_to_df(page_num)
            print(tabulate(df, headers='keys', tablefmt='psql'))  # psql-style table format
        else:
            break


def run_search_name():
    b = Bohum()



if __name__ == '__main__':
    print('!  Get Datagokr Data...')
    ask_ = input('\n '
                 '\n  1. 데이터 다운로드 '
                 '\n  2. 데이터 확인'
                 '\n  3. 사업자 이름 검색'
                 '\n  4. 사업자 등록번호 검색'
                 '\n  > ')
    if ask_ == '1':
        run_cli_bohum()
    elif ask_ == '3':
        run_search_name()
