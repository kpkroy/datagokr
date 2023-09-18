from api_juso import JusoAddr


if __name__ == '__main__':
    wd = 'data'
    a = JusoAddr()
    a.call_api(juso)
    a.get_result()