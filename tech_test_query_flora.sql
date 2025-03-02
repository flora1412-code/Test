---generate list of dates between 2020-06-01 and 2020-09-01 to cover data missing in any date
with dates_list as (
SELECT generate_series(
    '2020-06-01'::TIMESTAMP,  -- Start date
    '2020-09-30'::TIMESTAMP,  -- End date
    '1 day'::INTERVAL    -- Step size (1 day)
)::date as dt_report),

---data quality issue found by qa script, duplicate user login_hash in users table, perform below to dedupe
users_dedupe as (
select 
login_hash,
server_hash,
country_hash,
currency,
enable,
row_number()over(partition by login_hash order by server_hash desc) as dedupe_number
from public.users u where enable = 1
),

---generate base data, including distinct combination values of dt_report, login_hash, server_hash, symbol, currency
base_data as (
select dl.dt_report,x.login_hash, x.server_hash,x.symbol,x.currency
from dates_list dl 
left join (select distinct 
	t.login_hash,
	t.server_hash,
	t.symbol,
	u.currency
from public.trades t inner join users_dedupe u on t.login_hash = u.login_hash) x on 1=1 ),

---generate daily volume and trade count as the base table
daily_volume as (
select 
dl.dt_report,
dl.login_hash,
dl.server_hash,
dl.symbol,
dl.currency,
coalesce(sum(st.volume),0) as total_trade_volume,
count(distinct st.ticket_hash) as total_trade_count
from base_data dl 
left join (select t.*,u.currency from public.trades t inner join (select login_hash, currency from users_dedupe where dedupe_number = 1) u on u.login_hash = t.login_hash) st
on dl.dt_report = st.close_time ::date and dl.login_hash=st.login_hash and dl.server_hash=st.server_hash and dl.symbol=st.symbol
---where dl.dt_report =  '2020-08-19'
---where dl.login_hash = 'C11D75E453A751014E6966E47AE08711'
group by dl.dt_report,
dl.login_hash,
dl.server_hash,
dl.symbol,
dl.currency)

select dv.*,
	   (select sum(total_trade_volume) 
	   	from daily_volume 
	   	where login_hash = dv.login_hash 
			and server_hash = dv.server_hash 
			and symbol = dv.symbol 
			and dt_report >= dv.dt_report - interval '7 days' and dt_report<=dv.dt_report) as sum_volume_prev_7d,
	   (select sum(total_trade_volume) 
	   		from daily_volume 
	   	where login_hash = dv.login_hash 
			and server_hash = dv.server_hash 
			and symbol = dv.symbol
			and dt_report<=dv.dt_report) as sum_volume_prev_all,
			
		(select volume_rank from (select dt_report,
				login_hash,
				symbol,
				dense_rank()over(order by sum(total_trade_volume) desc) as volume_rank
	   	from daily_volume 
	   	where login_hash = dv.login_hash 
			and symbol = dv.symbol 
			and dt_report >= dv.dt_report - interval '7 days' and dt_report<=dv.dt_report
			group by dt_report,login_hash,symbol) x where x.dt_report = dv.dt_report ) as rank_volume_symbol_prev_7d,

		(select count_rank from (select dt_report,login_hash,
				dense_rank()over(order by sum(total_trade_count) desc) as count_rank
	   	from daily_volume 
	   	where login_hash = dv.login_hash 
			and dt_report >= dv.dt_report - interval '7 days' and dt_report<=dv.dt_report
			group by dt_report,login_hash) x where x.dt_report = dv.dt_report) as rank_count_prev_7d,
	   (select sum(total_trade_volume) 
	   	from daily_volume 
	   	where login_hash = dv.login_hash 
			and server_hash = dv.server_hash 
			and symbol = dv.symbol 
			and dt_report between '2020-08-01' and dv.dt_report and dv.dt_report between '2020-08-01' and '2020-08-31') as sum_volume_2020_08,
	   (select min(dt_report) 
	   	from daily_volume 
	   	where login_hash = dv.login_hash 
			and server_hash = dv.server_hash 
			and symbol = dv.symbol 
			and dt_report<=dv.dt_report) as date_first_trade,
		row_number()over(order by dt_report,login_hash,server_hash,symbol) as row_number
from daily_volume dv;
---order by dt_report,login_hash,server_hash,symbol;
---where dv.dt_report ='2020-08-14';
