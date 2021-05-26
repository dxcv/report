import os
import pandas as pd
import pymysql
from WindPy import *
from docx import *
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT


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
                tmp['业务日期'] = pd.to_datetime(tmp['业务日期'], format='%Y-%m-%d')
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
                tmp.loc[tmp['产品分类'] == '债券', '名称'] = tmp.loc[tmp['产品分类'] == '债券', '名称'].map(
                    lambda lambda_x: lambda_x[:lambda_x.rfind("(")])
                if not w.isconnected():
                    w.start()
                exchange = w.wss("USDCNY.EX", "close", "tradeDate=" +
                                 str(self.data.loc[0, '业务日期'].date()).replace("-", "") + ";priceAdj=U;cycle=D")
                for x in tmp[tmp['投组单元名称'].str.contains('美元')].index.tolist():
                    tmp.loc[x, '市值'] = tmp.loc[x, '市值'] * exchange.Data[0][0]
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
            db = pymysql.connect(host='localhost', port=3306, user='root', password='root', db='pac', charset='utf8')
            instrument = pd.read_sql("select name from instrument_am", db)['name'].tolist()
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
                for x in instrument:
                    tmp.loc[tmp['名称'] == x, '产品分类'] = '理财直接融资工具'
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
                for x in instrument:
                    tmp.loc[tmp['名称'] == x, '产品分类'] = '理财直融工具'
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
        self.name = {"同业业务中心": self.TY(), "理财事业部": self.LC()}.get(name)

    def get_asset(self):
        return self.name.asset

    def get_loan(self):
        return self.name.loan

    def get_stream(self):
        return self.name.stream


class Bond:
    def __init__(self, bond):
        self.bond = bond
        self.flag = False

    def code(self):
        db = pymysql.connect(host='localhost', port=3306, user='root', password='root', db='pac', charset='utf8')
        code = pd.read_sql("select * from bond_code where name in ('" + "','".join(self.bond['名称'].tolist()) + "')", db)
        data = pd.merge(self.bond, code, how="left", left_on="名称", right_on="name")
        cur = db.cursor()
        for line in data[data['code'].isna()].index.tolist():
            new_name = data.loc[line, '名称']
            new_code = input(new_name + "对应的代码：")
            cur.execute("insert into bond_code values('" + new_name + "','" + new_code + "')")
            data.loc[line, 'code'] = new_code
        db.commit()
        db.close()
        data['债券代码'] = data['code']
        self.bond = data[self.bond.columns.tolist() + ['债券代码']].copy(deep=True)

    def asset_bond(self):
        self.code()
        if not w.isconnected():
            w.start()
        data = w.wss(",".join(set(self.bond['债券代码'].tolist())),
                     "windl1type,windl2type,province,city,comp_name,municipalbond,subordinateornot,mixcapital,perpetualornot,issue_issuemethod,modidura_cnbd,net_cnbd,latestpar,amount,latestissurercreditrating,issueamount",
                     "unit=1;tradeDate=" + str(self.bond.loc[0, '业务日期']).split()[0].replace("-", "") + ";credibility=1",
                     usedf=True)[1]
        wind_columns = ['WIND一级分类', 'WIND二级分类', '省份', '城市', '发行主体', '是否城投债', '是否次级债', '是否混合资本债券', '是否永续债', '发行方式',
                        '修正久期', '估值净价', '最新面值', '债项评级', '主体评级', '发行总额']
        data.columns = wind_columns
        data = pd.merge(self.bond, data, how='left', left_on='债券代码', right_index=True)
        self.bond = data[self.bond.columns.tolist() + wind_columns]
        self.bond.loc[self.bond['WIND一级分类'].str.contains('国债|地方政府债|央行票据|政府支持机构债'), '债券类别'] = '利率债'
        self.bond.loc[(self.bond['WIND一级分类'] == '金融债') & (self.bond['WIND二级分类'] == '政策银行债'), '债券类别'] = '利率债'
        self.bond.loc[(self.bond['债券类别'].isna()) & (self.bond['WIND一级分类'] == '金融债'), '债券类别'] = '金融债'
        self.bond.loc[self.bond['WIND一级分类'] == '同业存单', '债券类别'] = '金融债'
        self.bond.loc[self.bond['债券类别'].isna(), '债券类别'] = '非金融企业债券'
        db = pymysql.connect(host='localhost', port=3306, user='root', password='root', db='pac', charset='utf8')
        special = pd.read_sql("select * from bond_special_info", db)
        for name in special['code'].tolist():
            self.bond.loc[self.bond['名称'] == name, '发行主体'] = special.loc[special['code'] == name, 'name']
            self.bond.loc[self.bond['名称'] == name, '是否城投债'] = special.loc[
                special['code'] == name, 'municipalbond']
            self.bond.loc[self.bond['名称'] == name, '发行方式'] = special.loc[
                special['code'] == name, 'issuemethod']
            self.bond.loc[self.bond['名称'] == name, 'WIND一级分类'] = special.loc[
                special['code'] == name, 'windl1type']
            self.bond.loc[self.bond['名称'] == name, '省份'] = special.loc[special['code'] == name, 'province']
            self.bond.loc[self.bond['名称'] == name, '城市'] = special.loc[special['code'] == name, 'city']
            self.bond.loc[self.bond['名称'] == name, '最新面值'] = special.loc[special['code'] == name, 'latestpar']
            self.bond.loc[self.bond['名称'] == name, '发行总额'] = special.loc[
                special['code'] == name, 'issueamount']
        self.flag = True
        return self.bond

    def asset_credit(self):
        if not self.flag:
            self.asset_bond()
        return self.bond[self.bond['债券类别'] == '非金融企业债券']

    def asset_abs(self):
        if not self.flag:
            self.asset_bond()
        data = self.bond[self.bond['WIND一级分类'] == '资产支持证券']
        if not w.isconnected():
            w.start()
        abs_type = w.wss(",".join(set(data['债券代码'].tolist())), "us_type", usedf=True)[1]
        abs_type.columns = ['ABS基础资产类型']
        data = pd.merge(data, abs_type, how='left', left_on='债券代码', right_index=True)
        return data

    def stream_get(self):
        self.code()
        if not w.isconnected():
            w.start()

        return self.bond


class MMF:
    def __init__(self, data):
        self.data = data

    def wind(self):
        if not w.isconnected():
            w.start()
        data = w.wss("".join(self.data['货基代码'].tolist()), "fund_corp_fundmanagementcompany,fund_fundscale", "unit=1",
                     usedf=True)[1]
        data.columns = ['管理人', '基金规模']
        self.data = pd.merge(self.data, data, left_on='货基代码', right_index=True)
        self.data['占比'] = self.data['市值'] * 100000000 / self.data['基金规模']

    def ratio(self):
        self.wind()
        out = [self.data['占比'].max(), self.data['占比'].min(), str(len(set(self.data['管理人'].tolist()))) + "家"]
        res = self.data.groupby('管理人')['市值'].sum()
        out.append(res.max())
        out.append(res.min())
        return out


class BalanceSheet:
    def __init__(self, asset, loan):
        self.asset = asset()
        if '业务日期' not in self.asset.columns.tolist():
            self.asset['业务日期'] = pd.to_datetime(input("请输入资产日期（%Y-%m-%d）："), format='%Y-%m-%d')
        self.loan = loan()
        self.bond = Bond(self.asset[self.asset['产品分类'] == '债券'].copy(deep=True))


class Stream:
    def __init__(self, stream):
        self.stream = stream()
        # self.bond = Bond(self.stream[self.stream[''] == '债券'].copy(deep=True)).stream_get()


class Department:
    def __init__(self, name):
        self.name = name
        func = Function(name)
        self.bs = BalanceSheet(func.get_asset(), func.get_loan())
        self.stream = Stream(func.get_stream())

    def struct(self):
        data = {}
        tmp = self.bs.asset.groupby('产品分类', as_index=False)['市值'].sum()
        tmp['占比'] = tmp['市值'] / tmp['市值'].sum()
        data['资产'] = tmp
        tmp = self.bs.loan.groupby('产品分类', as_index=False)['市值'].sum()
        tmp['占比'] = tmp['市值'] / tmp['市值'].sum()
        data['负债'] = tmp
        return data

    def concentration(self):
        res = self.bs.bond.asset_credit()
        out = []
        data = res.groupby('发行主体')['市值'].sum()
        big = []
        tmp = list(set(data.sort_values(ascending=False).index.tolist()))[:10]
        for x in tmp:
            big.append([x, str(round(data[x], 2)), str(len(res[res['发行主体'] == x]))])
        level = [set(data[data <= 0.5].index.tolist()), set(data[(data > 0.5) & (data <= 1)].index.tolist()),
                 set(data[(data > 1) & (data <= 2)].index.tolist()), set(data[data > 2].index.tolist())]
        for x in range(len(level)):
            tmp = res[res['发行主体'].isin(level[x])]
            out.append([str(len(level[x])), str(len(tmp)), str(round(tmp['市值'].sum(), 2)),
                        str(round(tmp['市值'].sum() / res['市值'].sum() * 100, 2)) + '%'])
        return out, big

    def area(self):
        res = self.bs.bond.asset_credit()
        res = res[res['省份'] == '福建省']
        data = res.groupby('城市', as_index=False)['市值'].sum()
        data['占比'] = data['市值'] / data['市值'].sum()
        return data

    def duration(self):
        res = self.bs.bond.asset_bond()
        return [[res['市值'].sum(), (res['市值'] * res['修正久期']).sum() / res['市值'].sum()],
                [res[res['债券类别'] == '利率债']['市值'].sum(),
                 (res[res['债券类别'] == '利率债']['市值'] * res['修正久期']).sum() / res[res['债券类别'] == '利率债']['市值'].sum()],
                [res[res['债券类别'] == '金融债']['市值'].sum(),
                 (res[res['债券类别'] == '金融债']['市值'] * res['修正久期']).sum() / res[res['债券类别'] == '金融债']['市值'].sum()],
                [res[res['债券类别'] == '非金融企业债券']['市值'].sum(),
                 (res[res['债券类别'] == '非金融企业债券']['市值'] * res['修正久期']).sum() / res[res['债券类别'] == '非金融企业债券'][
                     '市值'].sum()]]

    def lever(self):
        pass

    def ratio(self):
        year = self.bs.asset.loc[0, '业务日期'].date().year
        month = (self.bs.asset.loc[0, '业务日期'].date().month - 1) // 3 * 3
        if month == 0:
            year -= 1
            month = 12
        day = {3: 31, 6: 30, 9: 30, 12: 31}.get(month)

        db = pymysql.connect(host='localhost', port=3306, user='root', password='root', db='pac', charset='utf8')
        cur = db.cursor()
        cur.execute("select val from blancesheet where date='" + str(year) + "/" + str(month) + "/" + str(
            day) + "' and name='总资产'")
        total = float(cur.fetchall()[0][0]) / 10000
        out = []
        res = self.bs.bond.asset_bond()
        res['评级'] = res['主体评级']
        res.loc[res['评级'].isna(), '评级'] = res[res['评级'].isna(), '主体评级']
        res['评级AA+以下'] = '是'
        res[res['评级'] == 'AAA', '评级AA+以下'] = '否'
        res[res['评级'] == 'AA+', '评级AA+以下'] = '否'
        total_bond = res['市值'].sum()
        out.append(str(round(total_bond / total * 100, 2)) + "%")
        total_credit = res.loc[res['债券类别'] == '非金融企业债券', '市值'].sum()
        out.append(str(round(total_credit / total_bond * 100, 2)) + "%")
        out.append(str(round(res.loc[res['评级AA+以下'] == '是', '市值'].sum() / total_bond * 100, 2)) + "%")
        out.append(str(round(res.loc[res['是否城投债'] == '是', '市值'].sum() / total_bond * 100, 2)) + "%")
        out.append(str(round(res.loc[res['发行方式'] == '私募', '市值'].sum() / total_bond * 100, 2)) + "%")
        out.append(str(round(res.loc[res['是否永续债'] == '是', '市值'].sum() / total_bond * 100, 2)) + "%")
        out.append(str(round(res.loc[res['是否次级债'] == '是', '市值'].sum() / total_bond * 100, 2)) + "%")
        out.append(str(round(res.loc[res['是否混合资本债券'] == '是', '市值'].sum() / total_bond * 100, 2)) + "%")
        return out

    def fund(self):
        pass

    def etf(self):
        pass


class Word:
    def __init__(self):
        self.document = Document('风险管理部金融市场风险监测报告模板.docx')

    @staticmethod
    def delete_row(table, n):
        row = table.rows[n]
        tbl = table._tbl
        tr = row._tr
        tbl.remove(tr)

    def sharp_table(self, table, n):
        total = len(table.rows)
        if total > n:
            for x in range(total - n):
                self.delete_row(table, 2)
        elif total < n:
            raise ValueError

    @staticmethod
    def style_cell(cell, name, size, bold=False):
        for paragraph in cell.paragraphs:
            paragraph.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
            for run in paragraph.runs:
                run.font.name = name
                run.font.size = size
                run.font.bold = bold

    def go(self):
        ty = Department("同业业务中心")
        lc = Department("理财事业部")

        size = len(ty.struct().get('资产'))
        if len(ty.struct().get('负债')) > size:
            size = len(ty.struct().get('负债'))
        self.sharp_table(self.document.tables[4], size + 4)
        for x in range(len(ty.struct().get('资产'))):
            self.document.tables[4].cell(2 + x, 0).text = ty.struct().get('资产').loc[x, '产品分类']
            self.style_cell(self.document.tables[4].cell(2 + x, 0), '宋体', 177800)
            self.document.tables[4].cell(2 + x, 1).text = str(round(ty.struct().get('资产').loc[x, '市值'], 2))
            self.style_cell(self.document.tables[4].cell(2 + x, 1), '宋体', 177800)
            self.document.tables[4].cell(2 + x, 2).text = str(round(ty.struct().get('资产').loc[x, '占比'] * 100, 2)) + "%"
            self.style_cell(self.document.tables[4].cell(2 + x, 2), '宋体', 177800)
        self.document.tables[4].cell(2 + size, 1).text = str(round(ty.struct().get('资产')['市值'].sum(), 2))
        self.style_cell(self.document.tables[4].cell(2 + size, 1), '宋体', 177800, True)
        for x in range(len(ty.struct().get('负债'))):
            self.document.tables[4].cell(2 + x, 3).text = ty.struct().get('负债').loc[x, '产品分类']
            self.style_cell(self.document.tables[4].cell(2 + x, 3), '宋体', 177800)
            self.document.tables[4].cell(2 + x, 4).text = str(round(ty.struct().get('负债').loc[x, '市值'], 2))
            self.style_cell(self.document.tables[4].cell(2 + x, 4), '宋体', 177800)
            self.document.tables[4].cell(2 + x, 5).text = str(round(ty.struct().get('负债').loc[x, '占比'] * 100, 2)) + "%"
            self.style_cell(self.document.tables[4].cell(2 + x, 5), '宋体', 177800)
        self.document.tables[4].cell(2 + size, 4).text = str(round(ty.struct().get('负债')['市值'].sum(), 2))
        self.style_cell(self.document.tables[4].cell(2 + size, 4), '宋体', 177800, True)

        size = len(lc.struct().get('资产'))
        if len(lc.struct().get('负债')) > size:
            size = len(lc.struct().get('负债'))
        self.sharp_table(self.document.tables[5], size + 4)
        for x in range(len(lc.struct().get('资产'))):
            self.document.tables[5].cell(2 + x, 0).text = lc.struct().get('资产').loc[x, '产品分类']
            self.style_cell(self.document.tables[5].cell(2 + x, 0), '宋体', 177800)
            self.document.tables[5].cell(2 + x, 1).text = str(round(lc.struct().get('资产').loc[x, '市值'], 2))
            self.style_cell(self.document.tables[5].cell(2 + x, 1), '宋体', 177800)
            self.document.tables[5].cell(2 + x, 2).text = str(round(lc.struct().get('资产').loc[x, '占比'] * 100, 2)) + "%"
            self.style_cell(self.document.tables[5].cell(2 + x, 2), '宋体', 177800)
        self.document.tables[5].cell(2 + size, 1).text = str(round(lc.struct().get('资产')['市值'].sum(), 2))
        self.style_cell(self.document.tables[5].cell(2 + size, 1), '宋体', 177800, True)
        for x in range(len(lc.struct().get('负债'))):
            self.document.tables[5].cell(2 + x, 3).text = lc.struct().get('负债').loc[x, '产品分类']
            self.style_cell(self.document.tables[5].cell(2 + x, 3), '宋体', 177800)
            self.document.tables[5].cell(2 + x, 4).text = str(round(lc.struct().get('负债').loc[x, '市值'], 2))
            self.style_cell(self.document.tables[5].cell(2 + x, 4), '宋体', 177800)
            self.document.tables[5].cell(2 + x, 5).text = str(round(lc.struct().get('负债').loc[x, '占比'] * 100, 2)) + "%"
            self.style_cell(self.document.tables[5].cell(2 + x, 5), '宋体', 177800)
        self.document.tables[5].cell(2 + size, 4).text = str(round(lc.struct().get('负债')['市值'].sum(), 2))
        self.style_cell(self.document.tables[5].cell(2 + size, 4), '宋体', 177800, True)

        data, big = ty.concentration()
        for x in range(4):
            for y in range(4):
                self.document.tables[13].cell(2 + x, y).text = data[x][y]
        for x in range(10):
            for y in range(3):
                self.document.tables[14].cell(2 + x, y).text = big[x][y]
        data, big = lc.concentration()
        for x in range(4):
            for y in range(4):
                self.document.tables[16].cell(2 + x, y).text = data[x][y]
        for x in range(10):
            for y in range(3):
                self.document.tables[17].cell(2 + x, y).text = big[x][y]

        data = ty.area()
        for x in range(len(data)):
            for y in range(3):
                self.document.tables[15].cell(2 + x, y).text = data.loc[x, data.columns.tolist()[y]]
        data = lc.area()
        for x in range(len(data)):
            for y in range(3):
                self.document.tables[18].cell(2 + x, y).text = data.loc[x, data.columns.tolist()[y]]

        data = ty.duration()
        for x in range(4):
            for y in range(2):
                self.document.tables[19].cell(1 + x, 1 + y).text = data[x][y]
        data = lc.duration()
        for x in range(4):
            for y in range(2):
                self.document.tables[19].cell(5 + x, 1 + y).text = data[x][y]

        # level table[20]

        data = ty.ratio()
        for x in range(8):
            self.document.tables[21].cell(1 + x, 1).text = data[x]
        data = lc.ratio()
        for x in range(8):
            self.document.tables[21].cell(1 + x, 2).text = data[x]

        data = MMF(ty.bs.asset[ty.bs.asset['产品类别'] == '货币基金']).ratio()
        for x in range(5):
            self.document.tables[22].cell(2 + x, 1).text = data[x]

        self.document.tables[23].cell(1, 1).text = self.document.tables[19].cell(1, 2).text
        self.document.tables[23].cell(2, 1).text = self.document.tables[19].cell(5, 2).text
        # 1 2

        if os.path.exists('风险管理部金融市场风险监测报告.docx'):
            os.remove('风险管理部金融市场风险监测报告.docx')
        self.document.save('风险管理部金融市场风险监测报告.docx')


if __name__ == '__main__':
    Word().go()
