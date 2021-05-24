import os
import pandas as pd
import pymysql


class Function:
    class TY:
        def __init__(self):
            self.data = pd.DataFrame(columns=['业务日期', '投组单元名称', '产品分类', '名称', '市值', '到期日', '起息日', '成本', '估值'])
            self.bs()

        def bs(self):
            if os.path.exists("data/同业资产余额报表.xls"):
                tmp = pd.read_excel("data/同业资产余额报表.xls")
                tmp = tmp[tmp['资产分类'].str.contains('受益权|收益权')]
                tmp['投组单元名称'] = '线下'
                tmp['市值'] = tmp['市值(万元)'] / 10000
                tmp = tmp[['日期', '投组单元名称', '资产分类', '底层资产名称', '市值', '到期日', '起息日']]
                tmp.columns = ['业务日期', '投组单元名称', '产品分类', '名称', '市值', '到期日', '起息日']
                db = pymysql.connect(host='localhost', port=3306, user='root', password='root', db='cost',
                                     charset='utf8')
                pac = pd.read_sql("select * from cost_otc", db)
                pac.columns = ['name', '成本']
                tmp = pd.merge(tmp, pac, how='left', left_on='名称', right_on='name')
                tmp['产品分类'] = tmp['成本'].map(lambda x: '债券' if x > 0 else 'SPV')
                tmp = tmp[['业务日期', '投组单元名称', '产品分类', '名称', '市值', '到期日', '起息日', '成本']]
                self.data = self.data.append(tmp)
                self.data = self.data.reset_index(drop=True)
            if os.path.exists("data/指定成本与FIFO损益分析-新.xls"):
                tmp = pd.read_excel("data/指定成本与FIFO损益分析-新.xls")
                tmp = tmp[~tmp['Unnamed: 4'].isna()]
                tmp = tmp[(tmp['交易投组'] != '2同业-同业投资(jy2@xmrcb)') & (tmp['交易投组'] != '自营-债基-底层资产(林文妹)')]
                tmp = tmp[~tmp['交易投组'].str.contains('资金往来')]
                tmp = tmp[['交易投组', 'Unnamed: 4', '市值', '到期日', '起息日', '原始购入成本价', '市场净价']]
                tmp.columns = ['投组单元名称', '名称', '市值', '到期日', '起息日', '成本', '估值']
                tmp['市值'] = tmp['市值'] / 100000000
                tmp['产品分类'] = tmp['投组单元名称'].map(lambda x: {'自营-资金-质押式回购(林文妹)': '质押式回购',
                                                           '自营-资金-拆借(林文妹)': '同业拆借',
                                                           '流动性-资金-同业借款-小微转贷款(资金业务部)': '同业借款',
                                                           '自营-债券借贷(林文妹)': '债券借贷'}.get(x, '债券'))
                self.data = self.data.append(tmp)
                self.data = self.data.reset_index(drop=True)

        def asset(self):
            tmp = self.data[self.data['市值'] > 0].copy(deep=True)
            tmp['市值'] = abs(tmp['市值'])
            return tmp

        def loan(self):
            tmp = self.data[self.data['市值'] < 0].copy(deep=True)
            tmp['市值'] = abs(tmp['市值'])
            return tmp

        def stream(self):
            pass

    class LC:
        def __init__(self):
            self.data = pd.DataFrame(columns=['业务日期', '投组单元名称', '产品分类', '名称', '市值', '到期日', '起息日', '成本', '估值'])
            self.bs()

        def bs(self):
            if os.path.exists("data/估值余额查询.xls"):
                tmp = pd.read_excel("data/估值余额查询.xls")
                tmp = tmp[tmp['名称'] != '鑫安利得7号']
                tmp = tmp[(tmp['投组单元名称'] == '丰裕') | (tmp['投组单元名称'] == '鑫安利得7号') | (tmp['投组单元名称'] == '丰盈专属')]
                tmp = tmp[['业务日期', '投组单元名称', '产品分类', '名称', '市值(元)', '到期日', '建仓时间']]
                tmp['投组单元名称'].replace('鑫安利得7号', '丰裕', inplace=True)
                tmp['市值'] = tmp['市值(元)'] / 100000000
                tmp = tmp[['业务日期', '投组单元名称', '产品分类', '名称', '市值', '到期日', '建仓时间']]
                tmp.columns = ['业务日期', '投组单元名称', '产品分类', '名称', '市值', '到期日', '起息日']
                self.data = self.data.append(tmp)
                self.data = self.data.reset_index(drop=True)
            if os.path.exists("data/利率型.xls"):
                tmp = pd.read_excel("data/利率型.xls")
                tmp_index = tmp[~tmp['理财产品/内部投组名称'].isna()].index.tolist()
                tmp_index.append(len(tmp))
                for x in range(len(tmp_index) - 1):
                    title = tmp.loc[tmp_index[x], '理财产品/内部投组名称']
                    for line in range(tmp_index[x], tmp_index[x + 1]):
                        tmp.loc[line, '理财产品/内部投组名称'] = title
                tmp = tmp[~tmp['投资资产明细'].isna()]
                tmp = tmp[(tmp['理财产品/内部投组名称'] != '厦门农商丰裕理财计划') & (tmp['理财产品/内部投组名称'] != '厦门农商银行丰盈专属人民币理财计划')]
                tmp['市值'] = tmp['投资金额(万元)'] / 10000
                tmp['资产名称'].replace(float('nan'), "", inplace=True)
                tmp['资产名称'] = tmp['资产名称'].map(lambda lambda_x: lambda_x[:lambda_x.rfind("(")])
                tmp = tmp[['理财产品/内部投组名称', '投资资产明细', '资产名称', '市值', '到期日', '起息日', '买入价格/100元', '估值/100元']]
                tmp.columns = ['投组单元名称', '产品分类', '名称', '市值', '到期日', '起息日', '成本', '估值']
                tmp['业务日期'] = self.data.loc[0, '业务日期']
                self.data = self.data.append(tmp)
                self.data = self.data.reset_index(drop=True)
            if os.path.exists("data/净值型.xls"):
                tmp = pd.read_excel("data/净值型.xls")
                tmp_index = tmp[~tmp['理财产品/内部投组名称'].isna()].index.tolist()
                tmp_index.append(len(tmp))
                for x in range(len(tmp_index) - 1):
                    title = tmp.loc[tmp_index[x], '理财产品/内部投组名称']
                    for line in range(tmp_index[x], tmp_index[x + 1]):
                        tmp.loc[line, '理财产品/内部投组名称'] = title
                tmp = tmp[~tmp['投资资产分类'].isna()]
                tmp['市值'] = tmp['投资金额(万元)'] / 10000
                tmp['资产名称'].replace(float('nan'), "", inplace=True)
                tmp['资产名称'] = tmp['资产名称'].map(lambda lambda_x: lambda_x[:lambda_x.rfind("(")])
                tmp = tmp[['理财产品/内部投组名称', '投资资产分类', '资产名称', '市值', '到期日', '起息日', '买入价格', '百元估值']]
                tmp.columns = ['投组单元名称', '产品分类', '名称', '市值', '到期日', '起息日', '成本', '估值']
                tmp['业务日期'] = self.data.loc[0, '业务日期']
                self.data = self.data.append(tmp)
                self.data = self.data.reset_index(drop=True)

        def asset(self):
            tmp = self.data[self.data['市值'] > 0].copy(deep=True)
            tmp['市值'] = abs(tmp['市值'])
            return tmp

        def loan(self):
            tmp = self.data[self.data['市值'] < 0].copy(deep=True)
            tmp['市值'] = abs(tmp['市值'])
            return tmp

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
