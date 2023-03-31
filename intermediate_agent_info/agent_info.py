from sql_engine import connect
import pandas as pd

sql1 = f'''
SELECT 
        a.phone AS 客户手机号,
        a.union_id as 客户联合ID,
        a.name 客户姓名,
        a.level 客户等级,
        if(a.level<=2,a.phone,if(b.level<=2,b.phone,if(c.level<=2,c.phone,if(d.level<=2,d.phone,null)))) as 所属总代手机号,
        if(a.level<=2,a.level,if(b.level<=2,b.level,if(c.level<=2,c.level,if(d.level<=2,d.level,null)))) as 所属总代等级,
        if(a.level<=2,a.name,if(b.level<=2,b.name,if(c.level<=2,c.name,if(d.level<=2,d.name,null)))) as 所属总代姓名,
        if(a.level<=2,a.union_id,if(b.level<=2,b.union_id,if(c.level<=2,c.union_id,if(d.level<=2,d.union_id,null)))) as 所属总代联合ID,
        k.official_name as 所属官方,
        k.official_UN 官方联合ID,
        j.phone as 所属官方电话,
        e.所属联盟,
        e.总监ID,
        e.总监,
        e.大区ID,
        e.大区,
        e.区经ID,
        e.区经,
        e.客经ID,
        e.客经,
        e.客服ID,
        e.客服,
        e.省
FROM
        oryx_agent.agent_info a
        LEFT JOIN oryx_agent.agent_info b ON a.parent_user_id = b.user_id
        LEFT JOIN oryx_agent.agent_info c ON b.parent_user_id = c.user_id
        LEFT JOIN oryx_agent.agent_info d ON c.parent_user_id = d.user_id
        LEFT JOIN 
(SELECT
        user_id,
        max( CASE key_flag WHEN 'sale_director_name' THEN field_value ELSE null END ) AS 总监,
        max( CASE key_flag WHEN 'sale_director_no' THEN field_value ELSE null END ) AS 总监ID,
        max( CASE key_flag WHEN 'big_regional_name' THEN field_value ELSE null END ) AS 大区,
        max( CASE key_flag WHEN 'big_regional_no' THEN field_value ELSE null END ) AS 大区ID,
        max( CASE key_flag WHEN 'regional_manager_name' THEN field_value ELSE null END ) AS 区经,
        max( CASE key_flag WHEN 'regional_manager_no' THEN field_value ELSE null END ) AS 区经ID,
        max( CASE key_flag WHEN 'service_manager_name' THEN field_value ELSE null END ) AS 客经,
        max( CASE key_flag WHEN 'service_manager_no' THEN field_value ELSE null END ) AS 客经ID,
        max( CASE key_flag WHEN 'account_manager_name' THEN field_value ELSE null END ) AS 客服,
        max( CASE key_flag WHEN 'account_manager_no' THEN field_value ELSE null END ) AS 客服ID,
        max( CASE key_flag WHEN 'alliance_name' THEN field_value ELSE null END ) AS 所属联盟,
	max( CASE key_flag WHEN 'province' THEN field_value ELSE null END ) AS 省	
FROM
        oryx_agent.agent_ext
WHERE
        company_id = 12
        AND del_flag = 0
GROUP BY
        user_id) e ON if(a.level<=2,a.user_id,if(b.level<=2,b.user_id,if(c.level<=2,c.user_id,if(d.level<=2,d.user_id,a.user_id)))) = e.user_id
        LEFT JOIN oryx_agent.agent_info j ON if(a.level=1,a.user_id,if(a.level=2,a.organization_parent_user_id,if(b.level=1,b.user_id,if(b.level=2,b.organization_parent_user_id,if(c.level=1,c.user_id,if(c.level=2,c.organization_parent_user_id,if(d.level=1,d.user_id,if(d.level=2,d.organization_parent_user_id,a.organization_parent_user_id)))))))) = j.user_id
        LEFT JOIN oryx_bi.active_official_relation k ON j.union_id = k.official_UN
WHERE 
        a.company_id = 12
        AND (a.state = 1 or a.state is null)
        AND (b.state = 1 or b.state is null)
        AND a.level BETWEEN 1 and 5
        AND LENGTH(a.name) > 0
        '''
        
sql2 = f'''
select * from oryx_bi.imp_alter_alliancename
'''

bi_con = connect('bidata')
yx_con, server = connect('yx')
df1 = pd.read_sql(sql1, yx_con)
df2 = pd.read_sql(sql2, yx_con)
df = pd.merge(df1,df2,left_on='所属联盟',right_on='alliancename_full',how='left')
df['客户手机号'] = df['客户手机号'].astype('str')
df.drop_duplicates(['客户姓名','客户手机号'],inplace=True)
# yx_con.execute(
# 	f"DELETE FROM InterMediate_yx_erp_return_account_diff WHERE DATEDIFF(DD, trans_date, GETDATE()) BETWEEN {diff_days_start} AND {diff_days_end}")
# todo = set(data['trans_date'].astype('str').to_list())
# if len(todo) > 0:
# 	todo = "('"+"','".join(todo)+"')"
# 	bi_con.execute(
# 		f"DELETE FROM InterMediate_yx_erp_return_account_diff WHERE trans_date IN {todo}")
# if len(data) > 0:
df.to_sql("InterMediate_agent_fullinfo", bi_con,
                    index=False, if_exists='replace', method='multi', chunksize=500)

