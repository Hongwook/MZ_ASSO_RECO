import json

import pandas as pd
import pymysql


class Queries:

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        # 인스턴스 생성 시 입력하는 매개변수(start_date, end_date)의 활용이 필요한 쿼리변수들은 인스턴스 변수로 변경. 나머지는 클래스 변수로 활용
        self.analytics_query = self.analytics_query.format(start_date, end_date)
        self.cart_query = self.cart_query.format(start_date, end_date)
        self.recommender_query = self.recommender_query.format(start_date, end_date)

    # queries for type == 'cscart'
    analytics_query = '''
    SELECT a.order_id, a.user_id, a.product_id, a.product_name_kor,
    a.option_1_id, a.option_name_1_kor, a.variant_1_id, a.variant_1_name_kor,
    a.option_2_id, a.option_name_2_kor, a.variant_2_id, a.variant_2_name_kor,
    a.barcode, a.category_M, a.category_S, a.currency, a.purchased_at,
    a.product_price, a.marked_down_price, a.product_qty, a.order_qty, a.company_name
    FROM cscart_order_analytics a
    WHERE a.purchased_at between "{}" and "{}"
    ORDER BY a.purchased_at asc;
    '''

    cart_query = '''
    SELECT a.user_id, a.type, a.user_type, a.item_id, a.item_type, a.product_id, 
    a.amount, a.price, from_unixtime(a.timestamp+ 14 * 3600, '%Y-%m-%d %H:%i:%s') as add_time
    FROM cscart_user_session_products a
    WHERE from_unixtime(a.timestamp, '%Y-%m-%d') between "{}" and "{}";
    '''

    user_query = '''
    SELECT a.user_id, a.status, a.user_type, a.company_id, 
    from_unixtime(a.last_login+ 14 * 3600, '%Y-%m-%d %H:%i:%s') as last_login, 
    from_unixtime(a.timestamp+ 14 * 3600, '%Y-%m-%d %H:%i:%s') as register_time, 
    a.firstname, a.lastname, a.birthday, a.age_range, a.gender
    FROM cscart_users a;   
    '''

    review_query = '''
    SELECT A.post_id, A.thread_id, D.object_id, E.product, 
    from_unixtime(A.timestamp+ 14 * 3600, '%Y-%m-%d %H:%i:%s') as post_time,
    A.user_id, B.rating_value, C.message, A.status
    FROM cscart_discussion_posts A, cscart_discussion_rating B,
    cscart_discussion_messages C, 
    (SELECT thread_id, object_id, object_type
    FROM cscart_discussion
    WHERE object_type = 'P') D,
    (SELECT product_id, lang_code, product
    FROM cscart_product_descriptions
    WHERE lang_code = 'ko') E
    WHERE A.thread_id = B.thread_id 
    AND A.post_id = B.post_id
    AND A.post_id = C.post_id 
    AND A.thread_id = C.thread_id
    AND A.thread_id = D.thread_id
    AND D.object_id = E.product_id
    ORDER BY A.timestamp asc;    
    '''

    products_query = '''
    SELECT a.product_id, a.is_edp, a.status, a.hash_tag
    FROM cscart_products a;
    '''

    inventory_query = '''
    SELECT a.barcode, a.amount, from_unixtime(a.timestamp+ 14 * 3600, '%Y-%m-%d %H:%i:%s') as sys_time
    FROM cscart_product_options_inventory_histories a;
    '''

    brand_query = '''
    select cscart_product_features_values.product_id, cscart_product_feature_variant_descriptions.variant as brand
    from cscart_product_features_values
    left join cscart_product_feature_variant_descriptions
    on cscart_product_features_values.variant_id = cscart_product_feature_variant_descriptions.variant_id 
    and cscart_product_features_values.lang_code = cscart_product_feature_variant_descriptions.lang_code
    where cscart_product_features_values.feature_id = 19 and cscart_product_features_values.lang_code='ko';
    '''

    mainexposure_query = '''
    select *
    from cscart_main_recommend_product_histories;
    '''

    search_query = '''
    SELECT *, from_unixtime(a.timestamp+ 14 * 3600, '%Y-%m-%d %H:%i:%s') as sys_time
    FROM cscart_mz_search_histories a;
    '''

    # queries for type == 'analytics'
    competitors_price_query = '''
    SELECT date, competitor, product_id, krw_price, usd_price
    FROM monitor_competitorsproduct
    WHERE date = (SELECT max(date) FROM monitor_competitorsproduct);
    '''

    competitors_query = '''
    SELECT *
    FROM monitor_competitors
    '''

    mzprice_query = '''
    select product_id, date, krw_price
    from monitor_item
    join monitor_mzproduct 
    on monitor_item.id = monitor_mzproduct.item_id 
    where date = (select max(date) from monitor_mzproduct);
    '''

    category_query = '''
    SELECT catenum.product_id, catenum.category_id, catenum.link_type, catedesc.category
    FROM cscart_products_categories AS catenum
    LEFT JOIN cscart_category_descriptions AS catedesc
    ON catenum.category_id = catedesc.category_id
    WHERE catedesc.lang_code='ko';
    '''

    recommender_query = '''
    select *
    from cscart_users_recommenders
    where created_at between "{}" and "{}"
    '''

class DBImport():
    def __init__(self, db_type='cscart', db_info_path='mz_db_password.json'):
        # get database password
        with open(db_info_path, 'rb') as file:
            db_info_dict = json.load(file)
        if db_type == 'cscart':
            db_info_dict = db_info_dict['cscart']
        elif db_type == 'analytics':
            db_info_dict = db_info_dict['analytics']
        self.host = db_info_dict['host']
        self.port = db_info_dict['port']
        self.user = db_info_dict['user']
        self.__passwd = db_info_dict['passwd']  # 비밀번호에 해당하여 비공개 속성(private attribute) 으로 '__' 처리
        self.db = db_info_dict['db']


    def data_import(self, query):
        self.connection = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.__passwd,
                                          db=self.db)
        data = pd.read_sql(query, con=self.connection)
        self.connection.close() # connection은 init 할때 호출하기 때문에 여기서 close 하면 해당 함수 재사용 불가.
        return data
