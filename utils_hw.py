import re

import pandas as pd


class python2sql():
    """
    provides functions related to transform from python to sql.
    """
    def count_max_length(dataframe, col, dtype, sql='postgresql'):
        # python dataframe data type and sql data type matching dictionary
        python2sql_dict = {
            'postgresql': {
                'int': 'numeric',
                'bool': 'boolean',
                'bytes': 'bytea',
                'str': 'varchar',
                'float': 'numeric',
                'object': 'varchar'
            },
            'mysql': {
                'int': 'int',
                'bool': 'boolean',
                'bytes': 'varbinary',
                'str': 'varchar',
                'float': 'decimal',
                'object': 'varchar'
            }
        }  # sql 종류 많아지면 json file로 별도관리하면 편할듯

        # change python data type into sql data type
        if dtype in python2sql_dict[sql].keys():
            sql_dtype = python2sql_dict[sql][dtype]
        else:
            sql_dtype = 'varchar'

        if dtype in ['bool', 'bytes']:
            return sql_dtype
        else:
            if dtype == 'float':
                apply_col = dataframe[col][~dataframe[col].isnull()].apply(lambda x: abs(x)).astype(
                    'str')  # 자릿수만 확인위해 절대값 처리 후 str 변환
                max_length = apply_col.apply(lambda x: len(x)).max() - 1  # float 전체 길이 (. 제외)
                int_length = apply_col.apply(lambda x: x.find('.')).max()
                return f'{sql_dtype}({max_length}, {max_length - int_length})'

            elif dtype == 'int':
                apply_col = dataframe[col][~dataframe[col].isnull()].astype('str')
                max_length = apply_col.apply(lambda x: len(x)).max()
                return f'{sql_dtype}({round(max_length * 1.2)})'

            else:  # object
                apply_col = dataframe[col][~dataframe[col].isnull()].astype('str')
                max_length = apply_col.apply(lambda x: len(x)).max()
                return f'{sql_dtype}({round(max_length * 1.2)})'  # 1.2배정도 여유 길이 제공


    def dtype_to_sqltype(dataframe, sql='postgresql'):
        # preprocessing python data type
        typedf = pd.DataFrame(dataframe.dtypes).reset_index()
        typedf.columns = ['col', 'type']
        typedf['type'] = typedf['type'].astype('str').apply(lambda x: re.sub(r"[0-9]", "", x))

        # get max length of each column
        typedf['type'] = typedf.apply(lambda x: count_max_length(dataframe, x['col'], x['type'], sql), axis=1)

        return typedf


    def make_table_specification(dataframe, col_conditions=None, sql='postgresql', excel_name=None):
        table_spec = dtype_to_sqltype(dataframe, sql)
        table_spec = table_spec.rename(columns={'col': 'column_cd', 'type': 'column_type'})
        spec_cols = ['table_cd', 'table_nm', 'column_nm', 'column_constraint', 'primary_key', 'foreign_key',
                     'table_constraint', 'comment']
        for col in spec_cols:
            table_spec[col] = None
        if col_conditions:
            table_cols = ['table_cd', 'table_nm']
            if len(set(table_cols) & set(col_conditions.keys())) > 0:
                inter_table_cols = list(set(table_cols) & set(col_conditions.keys()))
                for table_col in inter_table_cols:
                    table_spec[table_col] = col_conditions[table_col]
            col_cols = ['column_nm', 'column_constraint', 'foreign_key', 'comment']
            if len(set(col_cols) & set(col_conditions.keys())) > 0:
                inter_col_cols = list(set(col_cols) & set(col_conditions.keys()))
                for col_col in inter_col_cols:
                    table_spec[col_col] = table_spec['column_cd'].apply(
                        lambda x: col_conditions[col_col][x] if x in col_conditions[col_col].keys() else None)
            if 'primary_key' in col_conditions.keys():
                pk_cols = col_conditions['primary_key']
                table_spec['primary_key'] = table_spec['column_cd'].apply(lambda x: 'Y' if x in pk_cols else None)
        table_spec = table_spec[
            ['table_cd', 'table_nm', 'column_cd', 'column_nm', 'column_type', 'column_constraint', 'primary_key',
             'foreign_key', 'comment']]
        if excel_name:
            table_spec.to_excel(excel_name + '.xlsx', index=False)
        return table_spec


    def table_create_query(dataframe, table_cd, col_conditions=None, sql='postgresql'):
        table_spec = make_table_specification(dataframe, col_conditions=col_conditions, sql=sql, excel_name=None)

        def column_preprocessing(table_row):
            preprocessed_row = table_row['column_cd'] + f" {str(table_row['column_type'])}"
            # column_constraint
            if pd.isna(table_row['column_constraint']):
                pass
            else:
                preprocessed_row = preprocessed_row + f" {str(table_row['column_constraint'])}"
            # foreign_key
            if pd.isna(table_row['foreign_key']):
                pass
            else:
                preprocessed_row = preprocessed_row + f" REFERENCES {str(table_row['foreign_key'])}"
            return "\n\t" + preprocessed_row + ","

        column_list = ''.join(table_spec.apply(lambda x: column_preprocessing(x), axis=1).tolist())
        pk_list = table_spec['column_cd'][~table_spec['primary_key'].isnull()].tolist()
        if len(pk_list) > 0:
            pk_list = ', '.join(pk_list)
            create_query = f"""
            CREATE TABLE {table_cd}({column_list}
            PRIMARY KEY ({pk_list})
            );
            """
        else:
            create_query = f"""
            CREATE TABLE {table_cd}({column_list}
            );
            """
        return create_query


    def table_insert_query(dataframe, table_cd, pks=None, sql='postgresql'):
        col_list = ', '.join(dataframe.columns)
        val_list = ', '.join(dataframe.columns.map(lambda x: f'%({x})s'))
        set_list = ', '.join(dataframe.columns.map(lambda x: f'{x} = %({x})s'))
        if sql=='postgresql':
            if pks:
                pk_list = ', '.join(pks)
                insert_query = f"""
                INSERT INTO {table_cd}({col_list})
                VALUES ({val_list})
                ON CONFLICT ({pk_list})
                DO UPDATE
                SET {set_list}
                ;
                """
            else:
                insert_query = f"""
                INSERT INTO {table_cd}({col_list})
                VALUES ({val_list})
                ;
                """
        elif sql=='mysql':
            insert_query = f"""
            INSERT INTO {table_cd}({col_list})
            VALUES ({val_list})
            ;"""
            # if pks:
            #     pk_list = ', '.join(pks)
            #     insert_query = f"""
            #     INSERT INTO {table_cd}({col_list})
            #     VALUES ({val_list})
            #     ON CONFLICT ({pk_list})
            #     DO UPDATE
            #     SET {set_list}
            #     ;
            #     """
            # else:
            #     insert_query = f"""
            #     INSERT INTO {table_cd}({col_list})
            #     VALUES ({val_list})
            #     ;
            #     """
        return insert_query