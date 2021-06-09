from report import Function
import pandas as pd
from datetime import datetime


class BankBook(Function.TY):
    def __init__(self):
        self.data = pd.DataFrame(columns=['业务日期', '投组单元名称', '产品分类', '名称', '市值', '到期日', '起息日', '成本', '估值'])
        self.data.loc[0, '业务日期'] = datetime.today()
        self.flow = pd.DataFrame(columns=['名称', '类别', '交易日', '方向', '金额', '交易投组', '对手方', '净价'])
        self.bs()

    def divide(self):
        trading = ['自营－现券－一级投资(暂停)(林文妹)',
                   '自营-现券-可供出售-信用债-同业投资(资金业务部)',
                   '自营-现券-可供出售-信用债2A（暂停）(林文妹)',
                   '自营-现券-可供出售-信用债(林文妹)',
                   '自营-现券-可供出售-美元债(林文妹)',
                   '自营-现券-可供出售-利率债3(林文妹)',
                   '自营-现券-可供出售-利率债2A（暂停）(林文妹)',
                   '自营-现券-可供出售-利率债(林文妹)',
                   '自营-现券-交易性-信用债2（暂停）(林文妹)',
                   '自营-现券-交易性-信用债2(暂停)(林文妹)',
                   '自营-现券-交易性-信用债(林文妹)',
                   '自营-现券-交易性-利率债3(林文妹)',
                   '自营-现券-交易性-利率债2（暂停)(林文妹)',
                   '自营-现券-交易性-利率债2（暂停）(林文妹)',
                   '自营-现券-交易性-利率债(林文妹)',
                   '自营-现券-持有到期-信用债(暂停)(林文妹)',
                   '自营-现券-持有到期-利率债(暂停)(林文妹)',
                   '自营-承销现券-可供出售-利率债(林文妹)',
                   '投行-现券-可供出售(林文妹)',
                   '同业-同业投资-深圳(林文妹)',
                   '同业-同业投资-北京(林文妹)']
        self.data = self.data[self.data['产品分类'] == '债券'].copy(deep=True)
        self.data['账簿分类'] = '银行账簿'
        for x in trading:
            self.data.loc[self.data['投组单元名称'] == x, '账簿分类'] = '交易账簿'

    def summary(self):
        return self.data.groupby('账簿分类')['市值'].sum()


if __name__ == '__main__':
    process = BankBook()
    process.divide()
    print(process.summary())
