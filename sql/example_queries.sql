-- Get all values - Every 10 secods
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


-- Get all values - Average on 1 hour
select
    n.node_name as node_name,
    n.description as node_description,
    s.description as sensor_description,
    date_trunc('hour', p.sensor_ts) as ts,
    avg(pd.r1) as signal_res,
    avg(pd.r2) as heater_res,
    avg(pd.volt) as volt,
    avg((p.attrs::json->>'G')::numeric) as g,
    avg((p.attrs::json->>'H')::numeric) as h,
    avg((p.attrs::json->>'P')::numeric) as p,
    avg((p.attrs::json->>'T')::numeric) as t,
    avg((p.attrs::json->>'RH')::numeric) as rh,
    avg((p.attrs::json->>'TH')::numeric) as th,
    avg((p.attrs::json->>'CFG')::numeric) as cfg,
    avg((p.attrs::json->>'CO2')::numeric) as co2,
    avg((p.attrs::json->>'IAQ')::numeric) as iaq,
    avg((p.attrs::json->>'VOC')::numeric) as voc,
    avg((p.attrs::json->>'IAC_comp')::numeric) as iac_comp
from packet_data pd
    left join packet p on p.id = pd.packet_id
    left join sensor s on s.id = pd.sensor_id
    left join node n on n.id = p.node_id
group by date_trunc('hour', p.sensor_ts), n.node_name, n.description, s.description
order by date_trunc('hour', p.sensor_ts);
