-- 查找函数1
-- cc:代码
-- dnlag down20的涨幅探测天数

CREATE OR REPLACE FUNCTION public.find(cc text, dnlag integer)
 RETURNS TABLE(code text, date date, rchannl real, rdown20 real, open real, low real, c_down20 real)
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
		     g as (select golang.date,golang.code,golang.open,golang.low from golang where golang.code=cc),
		     c as (select predict.date,predict.code,predict.c_down20 from predict where predict.code=cc)
		select g.code,g.date,
		       m.rchannl:: real,
		       (m.down20-m.down20_lag)/m.down20_lag*100 ::real as rdown20,
		       g.open::real,
		       g.low::real,
		       c.c_down20::real
		      from m,g,c
		      where m.tdate=g.date and
		            c.date=g.date and
		            g.low<c.c_down20
		      order by date desc;

	-- -------------------------------------------------------------------------------------------
return;
end;
$function$
;




-- =========================================================================================================================
-- =========================================================================================================================
--储存所有代码的find数据

CREATE OR REPLACE FUNCTION public.find_store(dnlag integer)
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
		open real,
		low real,
		c_down20 real
	    );
   for x in select stock_code.code from stock_code loop
    -- -------------------------------------------------------------------------------------------
         insert into findall select * from find(x,dnlag);
	-- -------------------------------------------------------------------------------------------
	end loop;
    create index findall_index on findall(code,date);
END
$function$
;

-- =========================================================================================================================
-- =========================================================================================================================
--验证函数
-- zy 止盈百分比
-- zs 止损百分比
-- rch boll通道宽度百分比
-- rbuy 购买点相对于c_down20下降百分比
-- rd20 boll下轨的平缓度

CREATE OR REPLACE FUNCTION public.verify(zy real, zs real, rch real, rbuy real, rd20 real)
 RETURNS TABLE(code text, buy_date date, sell_date date, buy real, sell real, profit real, info text)
 LANGUAGE plpgsql
 STRICT
AS $function$
declare
	x RECORD;
    y RECORD;
    buy real;
    issell bool;
begin
   for x in select * from findall where rchannl<=rch and rdown20>rd20 and (low-c_down20)*100/c_down20<=rbuy and open>c_down20 loop
    -- ---------------------------------------------------------------------------------------------------------------------------
       issell=false;
       buy=(x.c_down20*(1+rbuy/100))::real;
       raise notice '%',buy;
       for y in select * from golang where golang.code=x.code and date>x.date order by date limit 10 loop
	    -- ----------------------------------------------------------------------------------------------
            if (y.open<=buy*(1+zs/100)) then
	            -- 第二天开盘价小于止损价则以开盘价止损
	           issell=true; 
               return query select x.code,
                                   x.date as buy_date,
                                   y.date as sell_date,
                                   buy::real,
                                   y.open::real as sell,
                                   ((y.open-buy)*100/buy)::real as profit,
                                   ('open止损')::text as info;
                           EXIT;
            end if;	 
	    -- ----------------------------------------------------------------------------------------------	       
            if (y.open>buy*(1+zs/100)) and (y.low<=buy*(1+zs/100)) then
	           -- 第二天开盘价没有小于止损价但最低价小于止损价则以止损价止损
	           issell=true; 
               return query select x.code,
                                   x.date as buy_date,
                                   y.date as sell_date,
                                   buy::real,
                                   (buy*(1+zs/100))::real as sell,
                                   zs::real as profit,
                                   ('止损')::text as info;
                           EXIT;
            end if;
           -- ----------------------------------------------------------------------------------------------
             if (y.open>=buy*(1+zy/100)) then
	            -- 第二天开盘价大于止盈价 则以止盈价止盈
	           issell=true; 
               return query select x.code,
                                   x.date as buy_date,
                                   y.date as sell_date,
                                   buy::real,
                                   y.open::real as sell,
                                   ((y.open-buy)*100/buy)::real as profit,
                                   ('open盈利')::text as info;
                          EXIT;
            end if;          
           -- ----------------------------------------------------------------------------------------------
            if (y.open<buy*(1+zy/100)) and (y.high>=buy*(1+zy/100)) then
	           -- 第二天开盘价小于止盈价但最高价大于止盈价 则以止盈价止盈
	           issell=true; 
               return query select x.code,
                                   x.date as buy_date,
                                   y.date as sell_date,
                                   buy::real,
                                   (buy*(1+zy/100))::real as sell,
                                   zy::real as profit,
                                   ('盈利')::text as info;
                          EXIT;
            end if;
           
           
       end loop;
	-- -------------------------------------------------------------------------------------------
       if issell is false then
 	           issell=true; 
               return query select x.code,
                                   x.date as buy_date,
                                   y.date as sell_date,
                                   buy::real,
                                   y.close::real as sell,
                                   ((y.close-buy)*100/buy)::real as profit,
                                   ('到期'||((y.close-buy)*100/buy))::text as info;
       end if;
	end loop;

return;
end;
$function$
;









