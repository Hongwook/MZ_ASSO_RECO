import warnings
from datetime import datetime, timedelta

from database import *

warnings.filterwarnings("ignore")


def get_general_popular(mode='fixed'):
    ### data import
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365 * 2)).strftime('%Y-%m-%d')  # 최근 2년간의 판매 데이터를 바탕으로 연관분석 진행
    query_obj = Queries(start_date, end_date)
    db_obj = DBconnection(db_type='cscart')

    analytics = db_obj.data_import(query_obj.analytics_query)
    # 재고 테이블 로드 및 전처리
    inventory = db_obj.data_import(query_obj.inventory_query)
    inventory = inventory[inventory['sys_time'] == inventory['sys_time'].max()]
    prod_bar = analytics.sort_values(by='purchased_at', ascending=False)[['product_id', 'barcode']].drop_duplicates(
        subset='barcode')  # 가장 최근 기준으로 상품코드-바코드 매칭
    inventory = pd.merge(inventory, prod_bar, on='barcode', how='left').groupby('product_id')[
        'amount'].sum().reset_index()
    prod_catem = analytics.sort_values(by='purchased_at', ascending=False)[
        ['product_id', 'category_M', 'product_name_kor']].drop_duplicates(subset='product_id')
    gen_prods = analytics.groupby('product_id')['product_qty'].sum().reset_index()
    gen_prods = pd.merge(gen_prods, prod_catem, on='product_id', how='left')
    gen_prods = pd.merge(gen_prods, inventory, on='product_id', how='left')
    gen_prods['amount'] = gen_prods['amount'].fillna(0).astype('int')
    gen_prods = gen_prods[~(gen_prods['category_M'].isin(['단백질', '보충제', '미사용분류']))]
    # gen_prods = gen_prods.sort_values(by='product_qty', ascending=False).head(30)
    gen_prods = gen_prods.rename(columns={'amount': 'inventory_qty', 'product_qty': 'recent_sales'})
    gen_prods['system_date'] = end_date

    if mode == 'update':
        gen_prods = gen_prods[gen_prods['amount'] > 0]  # 현재고 있는 상품들 필터링
        gen_prods = gen_prods.sort_values(by='recent_sales', ascending=False).groupby('category_M').head(2)

    elif mode == 'fixed':
        fixed_prods = [4109, 3687, 1574, 2269, 2570, 1726, 3495, 470, 1696, 528, 110, 54, 5576, 1727, 2181, 733, 1129,
                       2263, 2151, 2495, 2210, 1728, 4619, 519,
                       3314, 4976, 2884, 884, 2477, 4427]
        gen_prods = gen_prods[gen_prods['product_id'].isin(fixed_prods)].sort_values(by='recent_sales', ascending=False)

    return gen_prods
