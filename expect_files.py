import great_expectations as gx

gx_df=gx.read_csv('files/tdcx_ruc_ente_temp_ced.txt',sep='|',dtype='str')

#print(gx_df)


# set_columns=['numcliente','ruccliente','numcuenta','estatus','balanceactual']
# print(gx_df.expect_table_columns_to_match_set(set(set_columns)))


# print(gx_df.expect_column_values_to_not_be_null('ruccliente',0.80))


# results=gx_df.expect_table_row_count_to_be_between(min_value=1)
# print(results["result"]['observed_value'])


print(gx_df.expect_column_value_lengths_to_equal('estatus', value=2,result_format='BOOLEAN_ONLY'))