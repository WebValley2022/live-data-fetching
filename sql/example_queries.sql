-- Get all values
select
    n.node_name as node_name,
    n.description as node_description,
    s.description as sensor_description,
    p.sensor_ts as ts,
    pd.r1 as signal_res,
    pd.r2 as heater_res,
    pd.volt as volt,
    p.attrs::json->'G' as g,
    p.attrs::json->'H' as h,
    p.attrs::json->'P' as p,
    p.attrs::json->'T' as t,
    p.attrs::json->'RH' as rh,
    p.attrs::json->'TH' as th,
    p.attrs::json->'CFG' as cfg,
    p.attrs::json->'CO2' as co2,
    p.attrs::json->'IAQ' as iaq,
    p.attrs::json->'VOC' as voc,
    p.attrs::json->'IAC_comp' as iac_comp
from packet_data pd
    left join packet p on p.id = pd.packet_id
    left join sensor s on s.id = pd.sensor_id
    left join node n on n.id = p.node_id
order by p.sensor_ts;


-- Group by minute, unable to average json values
select n.node_name,
    s.description,
    date_trunc('minute', p.sensor_ts) as ts,
    avg(pd.r1) as r1,
    avg(pd.r2) as r2,
    avg(pd.volt) as volt
from packet_data pd
    left join packet p on p.id = pd.packet_id
    left join sensor s on s.id = pd.sensor_id
    left join node n on n.id = p.node_id
group by date_trunc('minute', p.sensor_ts), n.node_name,s.description
order by date_trunc('minute', p.sensor_ts);
