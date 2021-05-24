import os
import pandas as pd


class Function:
    class TY:
        def asset(self):
            pass

        def loan(self):
            pass

        def stream(self):
            pass

    class LC:
        def asset(self):
            data = pd.DataFrame(columns=['业务日期', '投组单元名称', '产品分类', '名称', '市值', '到期日', '起息日', '成本', '估值'])
            if os.path.exists("data/估值余额查询.xls"):
                tmp = pd.read_excel("data/估值余额查询.xls")
                tmp = tmp[tmp['名称'] != '鑫安利得7号']
                tmp = tmp[(tmp['投组单元名称'] == '丰裕') | (tmp['投组单元名称'] == '鑫安利得7号') | (tmp['投组单元名称'] == '丰盈专属')]
                tmp = tmp[['业务日期', '投组单元名称', '产品分类', '名称', '市值(元)', '到期日', '建仓时间']]
                tmp['投组单元名称'].replace('鑫安利得7号', '丰裕', inplace=True)
                tmp['市值'] = tmp['市值(元)'] / 100000000
                tmp = tmp[['业务日期', '投组单元名称', '产品分类', '名称', '市值', '到期日', '建仓时间']]
                tmp.columns = ['业务日期', '投组单元名称', '产品分类', '名称', '市值', '到期日', '起息日']
                data = data.append(tmp)
            if os.path.exists("data/利率型.xls"):
                tmp = pd.read_excel("data/利率型.xls")
                tmp_index = tmp[~tmp['理财产品/内部投组名称'].isna()].index.tolist()
                tmp_index.append(len(tmp))
                for x in range(len(tmp_index) - 1):
                    title = tmp.loc[tmp_index[x], '理财产品/内部投组名称']
                    for line in range(tmp_index[x], tmp_index[x + 1]):
                        tmp.loc[line, '理财产品/内部投组名称'] = title
                tmp = tmp[~tmp['投资资产明细'].isna()]
                tmp['市值'] = tmp['投资金额(万元)'] / 10000
                tmp['资产名称'].replace(float('nan'), "", inplace=True)
                tmp['资产名称'] = tmp['资产名称'].map(lambda lambda_x: lambda_x[:lambda_x.rfind("(")])
                tmp = tmp[['理财产品/内部投组名称', '投资资产明细', '资产名称', '市值', '到期日', '起息日', '买入价格/100元', '估值/100元']]
                tmp.columns = ['投组单元名称', '产品分类', '名称', '市值', '到期日', '起息日', '成本', '估值']
                tmp['业务日期'] = data.loc[0, '业务日期']
                data = data.append(tmp)
            return data

        def loan(self):
            pass

        def stream(self):
            pass

    def __init__(self, name):
        self.name = {"同业": self.TY(), "理财": self.LC()}.get(name)

    def get_asset(self):
        return self.name.asset

    def get_loan(self):
        return self.name.loan

    def get_stream(self):
        return self.name.stream


class BalanceSheet:
    def __init__(self, asset, loan):
        self.asset = asset()
        self.loan = loan()


class Stream:
    def __init__(self, stream):
        self.stream = stream()


class Department:
    def __init__(self, name):
        func = Function(name)
        self.bs = BalanceSheet(func.get_asset(), func.get_loan())
        self.stream = Stream(func.get_stream())

    def struct(self):
        pass


if __name__ == '__main__':
    ty = Department("同业")
    lc = Department("理财")
