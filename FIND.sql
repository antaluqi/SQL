-- 查找函数1，通道宽率小于rch ,购买点小于c_down20*(1-rb/100),rch rb 为0~100的数字
-- cc:code
-- rch:rate of channl
-- rb rate of c_down20 (point of buy)

CREATE OR REPLACE FUNCTION public.find(cc text, rch integer, rb real, dnlag integer)
 RETURNS TABLE(code text, date date, rchannl real, rdown20 real, buy real, low real, c_down20 real, rlhh2 real, rlhh5 real, rlhh10 real, rc2 real, rc5 real, rc10 real)
 LANGUAGE plpgsql
 STRICT
AS $function$
declare
begin
    -- -------------------------------------------------------------------------------------------
       return query 
		with m as (select lag(maboll.date,-1) over(order by maboll.date) as tdate,
		                  maboll.code,
		                  maboll.down20,
		                  (maboll.up20-maboll.down20)/maboll.down20*100 as rchannl,
	                      lag(maboll.down20,dnlag) over(order by maboll.date) as down20_lag
	               from maboll where maboll.code=cc),  
		     g as (select golang.date,golang.code,golang.low from golang where golang.code=cc),
		     c as (select predict.date,predict.code,predict.c_down20,predict.c_down20*(1-rb/100) as buy from predict where predict.code=cc),
		     f as (select future.date,future.code,lhh2,lhh5,lhh10,c2,c5,c10 from future where future.code=cc)
		select g.code,g.date,
		       m.rchannl:: real,
		       (m.down20-m.down20_lag)/m.down20_lag*100 ::real as rdown20,
			   c.buy::real,
		       g.low::real,
		       c.c_down20::real,
		       ((f.lhh2-c.buy)/c.buy*100)::real as rlhh2,
		       ((f.lhh5-c.buy)/c.buy*100)::real as rlhh5,
		       ((f.lhh10-c.buy)/c.buy*100)::real as rlhh10,
		       ((f.c2-c.buy)/c.buy*100)::real as rc2,
		       ((f.c5-c.buy)/c.buy*100)::real as rc5,
		       ((f.c10-c.buy)/c.buy*100)::real as rc10
		      from m,g,c,f
		      where m.tdate=g.date and
		            c.date=g.date and
		            f.date=g.date and
		            m.rchannl<=rch and
		            g.low<c.buy
		      order by date desc;

	-- -------------------------------------------------------------------------------------------
return;
end;
$function$
;



-- =========================================================================================================================
-- =========================================================================================================================
--储存所有代码的find数据

CREATE OR REPLACE FUNCTION public.find_store(rch integer, rb real,dnlag int)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE --定义
x text; 
begin
   drop table if exists findall;
   create table if not exists findall(
		code text, 
		date date,
		rchannl real,
		rdown20 real,
		buy real,
		low real,
		c_down20 real,
		rlhh2 real,
		rlhh5 real,
		rlhh10 real,
		rc2 real,
		rc5 real,
		rc10 real
	    );
   for x in select stock_code.code from stock_code loop
    -- -------------------------------------------------------------------------------------------
         insert into findall select * from find(x,rch,rb,dnlag);
	-- -------------------------------------------------------------------------------------------
	end loop;
    create index findall_index on findall(code,date);
END
$function$
;


