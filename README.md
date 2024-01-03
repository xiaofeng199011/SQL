# SQL
#第一段
with para 
as (
select <_USER_> substr(?{_USER_}, 1, 1) || substr(?{_USER_}, instr(?{_USER_}, ' ') + 1, 8) as name 
</_USER_> from dual)
,t as (
select month_key
,cost_cnt
,0 as tke_used
,0 as rack_pdu_power
,0  as rack_id
,b.name,b.name_cn,b.dc_region_name_cn,b.dc_property,b.dc_level,b.istatus
,d.budget_calibre
,a.business_code as could_tag
from dm.dm_dc_tco_subject_m a
left join dm.dim_ecmdb_dc b            on a.dc_id=b.id
left join dm.dim_dc_tco_subject c     on a.subject_id=c.id
left join ods.ods_dc_budget_calibre d on  b.name=d.name 
where 1
and substr(month_key,1,4)=2023
--and subject_id not in ('027', '028', '033', '034', '064', '070','061', '666')
and control_level like '%TCO Management%'
and c.level_type_old ='数据中心'
and d.assignment='DC_Admin'

union

select distinct substr(bbq_d,1,6) as month_key
,0 as cost_cnt
,0 as tke_used
,0 as rack_pdu_power
,count(a.sn) over(partition by b.name_cn,month_key,a.cloud_type order by 1) as rack_id
,b.name,b.name_cn,b.dc_region_name_cn,b.dc_property,b.dc_level,b.istatus
,c.budget_calibre
,a.cloud_type as could_tag
from dm.dm_asset_detail_f a
left join dm.dim_ecmdb_dc b on a.dc_code=b.id
left join ods.ods_dc_budget_calibre c on  b.name=c.name 
where 1
and substr(bbq_d,1,4)=2023
and bbq_d=last_day(bbq_d::date)
and srcsys='CAM' 
and asset_name='PHY'
and tag_status_name_x in('已验收','已上架','已硬装')
and control_level like '%TCO Management%'
and c.assignment='DC_Admin'
group by month_key,cost_cnt,tke_used,rack_pdu_power,rack_id,a.sn,b.name,b.name_cn,dc_region_name_cn,dc_property,b.dc_level,b.istatus,c.budget_calibre,a.cloud_type

union

select substr(calendar_key,1,6) as month_key
,0 as cost_cnt
,tke_used
,rack_pdu_power
,0 as rack_id
,b.name,b.name_cn,b.dc_region_name_cn,b.dc_property,b.dc_level,b.istatus
,c.budget_calibre
,a.cloud_business_tag as could_tag
from dw.dws_fact_dc_cap_business a
left join dm.dim_ecmdb_dc b on a.id=b.id
left join ods.ods_dc_budget_calibre c on  b.name=c.name
where 1 
and substr(calendar_key,1,4)=2023
and calendar_key=last_day(calendar_key::date)
and c.assignment='DC_Admin'
and control_level like '%TCO Management%')

select month_key,cost_cnt,tke_used,rack_pdu_power,rack_id,name,name_cn,dc_region_name_cn,dc_level,istatus
,dc_property,budget_calibre,could_tag,e_name

from t
left join (select distinct s_name,e_name from ods.ods_ecmdb_relation_all_eng where e_subtype='Campus') s on s.s_name=t.name



#第二段
with q as(
select a.name
,a.type
,a.subtype
,a.roomname
,a.istatus
,a.dcname 
,a.install_date
,a.created_by_date
,(coalesce(install_date,created_by_date)) as time_start
,(time_start::date + interval '30 years') as time_over
from dm_ecmdb_transformer_v as a
union                                             
select b.name
,b.type
,b.subtype
,b.roomname
,b.istatus
,(coalesce(dcname,dc_name)) as dcname 
,b.install_date
,b.created_by_date
,(coalesce(install_date,created_by_date)) as time_start
,(time_start::date + interval '8 years') as time_over
from dm_ecmdb_ups_v as b
union                                             
select c.name
,c.type
,c.subtype
,c.roomname
,action as istatus
,c.dcname 
,created_by_date 
,null as install_date
,(coalesce(install_date,created_by_date)) as time_start
,(time_start::date + interval '5 years') as time_over
from dm_ecmdb_singlebattery_v as c
union                                             
select d.name
,d.type
,d.subtype
,d.roomname
,d.istatus
,d.dcname 
,d.install_date
,d.created_by_date
,(coalesce(install_date,created_by_date)) as time_start
,(time_start::date + interval '5 years') as time_over
from dm_ecmdb_batterygroup_v as d
union                                             
select e.name
,e.type
,e.subtype
,e.roomname
,e.istatus
,e.dcname 
,e.install_date
,e.created_by_date
,(coalesce(install_date,created_by_date)) as time_start
,(time_start::date + interval '30 years') as time_over
from dm_ecmdb_generator_v as e
union                                             
select f.name
,f.type
,f.subtype
,f.roomname
,f.istatus
,(coalesce(dcname,dc_name)) as dcname 
,f.install_date
,f.created_by_date
,(coalesce(install_date,created_by_date)) as time_start
,(time_start::date + interval '15 years') as time_over
from dm_ecmdb_aircondition_v as f
union                                             
select g.name
,g.type
,g.subtype
,g.roomname
,g.istatus
,g.dcname 
,g.install_date
,g.created_by_date
,(coalesce(install_date,created_by_date)) as time_start
,(time_start::date + interval '20 years') as time_over
from dm_ecmdb_coolingtower_v as g
union                                             
select h.name
,h.type
,h.subtype
,h.roomname
,h.istatus
,h.dcname 
,h.install_date
,h.created_by_date
,(coalesce(install_date,created_by_date)) as time_start
,(time_start::date + interval '30 years') as time_over
from dm_ecmdb_chiller_v as h
union                                             
select i.name
,i.type
,i.subtype
,i.roomname
,i.istatus
,(coalesce(dcname,dc_name)) as dcname 
,i.install_date
,i.created_by_date
,(coalesce(install_date,created_by_date)) as time_start
,(time_start::date + interval '15 years') as time_over
from dm_ecmdb_pdc_v as i
union                                             
select j.name
,j.type
,j.subtype
,j.roomname
,j.istatus
,(coalesce(dcname,dc_name)) as dcname 
,j.install_date
,j.created_by_date
,(coalesce(install_date,created_by_date)) as time_start
,(time_start::date + interval '8 years') as time_over
from dm_ecmdb_inframonitor_v as j
union                                             
select k.name
,k.type
,k.subtype
,k.roomname
,k.istatus
,k.dcname 
,k.install_date
,k.created_by_date
,(coalesce(install_date,created_by_date)) as time_start
,(time_start::date + interval '15 years') as time_over
from dm_ecmdb_themains_v as k
union                                             
select l.name
,l.type
,l.subtype
,l.roomname
,l.istatus
,l.dcname 
,l.install_date
,l.created_by_date
,(coalesce(install_date,created_by_date)) as time_start
,(time_start::date + interval '8 years') as time_over
from dm_ecmdb_video_surveillance_system_v as l
union                                             
select m.name
,m.type
,m.subtype
,m.roomname
,m.istatus
,m.dcname 
,m.install_date
,m.created_by_date
,(coalesce(install_date,created_by_date)) as time_start
,(time_start::date + interval '20 years') as time_over
from dm_ecmdb_pump_v as m
union                                             
select n.name
,n.type
,n.subtype
,n.roomname
,n.istatus
,n.dcname 
,n.install_date
,n.created_by_date
,(coalesce(install_date,created_by_date)) as time_start
,(time_start::date + interval '20 years') as time_over
from dm_ecmdb_tank_v as n
union                                             
select o.name
,o.type
,o.subtype
,o.roomname
,o.istatus
,o.dcname 
,o.install_date
,o.created_by_date
,(coalesce(install_date,created_by_date)) as time_start
,(time_start::date + interval '8 years') as time_over
from dm_ecmdb_cooling_control_system_v as o
union                                             
select p.name
,p.type
,p.subtype
,p.roomname
,p.istatus
,(coalesce(dcname,dc_name)) as dcname 
,p.install_date
,p.created_by_date
,(coalesce(install_date,created_by_date)) as time_start
,(time_start::date + interval '10 years') as time_over
from dm_ecmdb_transferswitch_v as p)
select 
q.type
,q.subtype
,q.roomname
,q.name
,q.istatus
,q.dcname
,q.time_start
,q.time_over
,age(time_over,now()) as rump_value
,(date_part('years',rump_value)*365+date_part('mons',rump_value)*30+date_part('days',rump_value)) as time_interval
,(case when (time_interval<0) then 1 else 0 end) as "已超期"
,(case when (time_interval>0 and time_interval<365) then 1 else 0 end) as "将要超期"
--,(case when (time_interval<0) then '已超期' when (time_interval>0 and time_interval<365.0) then '将要超期' else time_interval::varchar end) as expired 不报错
--,(case when (time_interval<0) then "已超期" when (time_interval>0 and time_interval<365.0) then "将要超期" else time_interval::varchar end) as expired 报错
,cast(q.type in ('Transformer','UPS','SingleBattery','BatteryGroup','Generator','PDC','TheMains','TransferSwitch') as int) as "供电组"
,cast(q.type in ('Aircondition','CoolingTower','Chiller','Pump','Tank','cooling_control_system','video_surveillance_system','InfraMonitor') as int) as "供冷组"
,cast(q.type='Transformer' as int)               "变压器"
,cast(q.type='UPS' as int)                       "UPS"
,cast(q.type='SingleBattery' as int)             "单体电池"
,cast(q.type='BatteryGroup' as int)              "电池组"
,cast(q.type='Generator' as int)                 "发电机"
,cast(q.type='PDC' as int)                       "配电柜"
,cast(q.type='InfraMonitor' as int)              "设施监控"
,cast(q.type='TheMains' as int)                  "市电输入"
,cast(q.type='video_surveillance_system' as int) "视频监控系统"
,cast(q.type='TransferSwitch' as int)            "转换开关"
,cast(q.type='Aircondition' as int)              "空调"
,cast(q.type='CoolingTower' as int)              "冷却塔"
,cast(q.type='Chiller' as int)                   "冷水机组"
,cast(q.type='Pump' as int)                      "油水泵"
,cast(q.type='Tank' as int)                      "油水罐"
,cast(q.type='cooling_control_system' as int)    "制冷群控制子系统"
,(case when r.dq is null then '/' else r.dq end) as dq
,(case when r.zd is null then '/' else r.zd end) as zd
,r.sfgjzd
,t.device_type
,t.device_type_cn
,t.device_group
from q
inner join (
select 'Transformer' as device_type,        '变压器' as device_type_cn,'供电设备' as device_group
union select 'UPS'                          ,'UPS'       ,'供电组'
union select 'SingleBattery'                ,'单体电池'         ,'供电组'
union select 'BatteryGroup'                 ,'电池组'           ,'供电组'
union select 'Generator'                    ,'发电机'           ,'供电组'
union select 'PDC'                          ,'配电柜'           ,'供电组'
union select 'InfraMonitor'                 ,'设施监控'         ,'供冷组'
union select 'TheMains'                     ,'市电输入'         ,'供电组'
union select 'video_surveillance_system'    ,'视频监控系统'     ,'供冷组'
union select 'TransferSwitch'               ,'转换开关'         ,'供电组'
union select 'Aircondition'                 ,'空调'             ,'供冷组'
union select 'CoolingTower'                 ,'冷却塔'           ,'供冷组'
union select 'Chiller'                      ,'冷水机组'         ,'供冷组'
union select 'Pump'                         ,'油水泵'           ,'供冷组'
union select 'Tank'                         ,'油水罐'           ,'供冷组'
union select 'cooling_control_system'       ,'制冷群控制子系统' ,'供冷组') as t on q.type=t.device_type
left join f_sjzxzdqd0901 as r on q.dcname=r.jc
--,(coalesce(dcname,dc_name)) as dcname 以dcname为准，dcname为空值，取dc_name值
--时间同理取值
--双引号报错，单引号，将符合条件的行变为'已超期'字符串，替换掉原来的内容

#第三段

