select n.node_name,s.description,
       date_trunc('minute', p.sensor_ts) as ts,avg(pd.r1) as r1,avg(pd.r2) as r2,avg(pd.volt) as volt
from packet_data pd
    left join packet p on p.id = pd.packet_id
    left join sensor s on s.id = pd.sensor_id
    left join node n on n.id = p.node_id
where pd.sensor_id = 1
group by date_trunc('minute', p.sensor_ts), n.node_name,s.description
order by date_trunc('minute', p.sensor_ts)