import warnings
from datetime import datetime, timedelta

import numpy as np

from database import *
from utils_hw import *

warnings.filterwarnings("ignore")

from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import association_rules, fpgrowth


def get_association_recommendation():
    ### data import
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365 * 2)).strftime('%Y-%m-%d')  # 최근 2년간의 판매 데이터를 바탕으로 연관분석 진행
    query_obj = Queries(start_date, end_date)
    dbimport_obj = DBImport(db_type='cscart')

    analytics = dbimport_obj.data_import(query_obj.analytics_query)
    # 재고 테이블 로드 및 전처리
    inventory = dbimport_obj.data_import(query_obj.inventory_query)
    inventory = inventory[inventory['sys_time'] == inventory['sys_time'].max()]
    prod_bar = analytics.sort_values(by='purchased_at', ascending=False)[['product_id', 'barcode']].drop_duplicates(
        subset='barcode')  # 가장 최근 기준으로 상품코드-바코드 매칭
    inventory = pd.merge(inventory, prod_bar, on='barcode', how='left').groupby('product_id')[
        'amount'].sum().reset_index()
    inventory = inventory.rename(columns={'product_id': 'consequents'})

    ### association analysis
    prod_order = analytics[['order_id', 'product_id']].drop_duplicates()
    item_list = prod_order.groupby('order_id')['product_id'].apply(list).tolist()

    te = TransactionEncoder()
    te_result = pd.DataFrame(te.fit_transform(item_list), columns=te.columns_)
    te_result.head()

    frequent_itemsets = fpgrowth(te_result, use_colnames=True, min_support=0.0001)
    result = association_rules(frequent_itemsets, metric="lift", min_threshold=1)

    # postprocessing
    prod_name = analytics.sort_values(by='purchased_at', ascending=False)[
        ['product_id', 'product_name_kor']].drop_duplicates(subset='product_id', keep='first')
    # change frozenset to value and filtering multi itemsets
    result['antecedents'] = result['antecedents'].apply(lambda x: list(x)[0] if len(list(x)) == 1 else 'multi')
    result['consequents'] = result['consequents'].apply(lambda x: list(x)[0] if len(list(x)) == 1 else 'multi')
    result = result[(result['antecedents'] != 'multi') & (result['consequents'] != 'multi')]
    result = pd.merge(result, prod_name, left_on='antecedents', right_on='product_id', how='left')
    result = result.rename(columns={'product_name_kor': 'ante_prod_nm'})
    result = pd.merge(result, prod_name, left_on='consequents', right_on='product_id', how='left')
    result = result.rename(columns={'product_name_kor': 'cons_prod_nm'})
    result = result.drop(['product_id_x', 'product_id_y'], axis=1)
    result = pd.merge(result, inventory, on='consequents', how='left')
    result['amount'] = result['amount'].fillna(0).astype('int')
    result = result[result['amount'] > 0]  # 현재 재고가 0이상인 상품기준 필터링
    result['lift'] = np.round(result['lift'], 2)
    result = result.rename(columns={'amount': 'inventory_qty'})
    result = result[
        ['antecedents', 'consequents', 'lift', 'ante_prod_nm', 'cons_prod_nm', 'inventory_qty']].sort_values(
        by=['antecedents', 'lift'], ascending=[True, False]).reset_index(drop=True)
    result['system_date'] = end_date

    return result