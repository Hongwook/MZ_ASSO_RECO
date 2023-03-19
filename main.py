import warnings
from datetime import datetime, timedelta

from association_recommendation import get_association_recommendation
from database import *
from general_popular import get_general_popular

warnings.filterwarnings("ignore")

# make db objects
end_date = datetime.now().strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=365 * 2)).strftime('%Y-%m-%d')  # 최근 2년간의 판매 데이터를 바탕으로 연관분석 진행
query_obj = Queries(start_date, end_date)

# run
asso_reco = get_association_recommendation()
gen_pop = get_general_popular(mode='fixed')

# data insert
dbinsert_obj = DBImport(db_type='analytics')  # dbimport_obj: cscart / dbinsert_obj: analytics
dbinsert_obj.data_insert(query_obj.association_reco_insert_query, asso_reco)
dbinsert_obj.data_insert(query_obj.association_gen_pop_insert_query, gen_pop)