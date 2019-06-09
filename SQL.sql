--N条记录 需要额外建立一个distinct code的表

create or replace function lastline(n integer) returns setof golang as $$
declare
x text;
begin
   for x in select code from stock_code loop
	     return query select * from golang where code=x order by date desc limit n;
    end loop;
return;
end;
$$ language plpgsql strict;


--=================================================================================================================================================
--=================================================================================================================================================

--最后的MA

CREATE OR REPLACE FUNCTION public.last_ma()
 RETURNS TABLE(code text, date date,price real, ma5 real,ma10 real, ma20 real, ma30 real, ma60 real,up real,down real)
 LANGUAGE plpgsql
 STRICT
AS $function$
declare
x text;
begin
   for x in select stock_code.code from stock_code loop
    -- -------------------------------------------------------------------------------------------
          return query 
			with a as (select *,rank() over(partition by golang.code order by golang.date desc) as rank from golang where golang.code =x order by golang.date desc limit 60)
			select d.code,
			       d.date,
				   d.close::real,
				   d5.ma5::real,
				   d10.ma10::real,
				   d20.ma20::real,
				   d30.ma30::real,
				   d60.ma60::real,
				   (d20.ma20+2*d20.std20)::real,
				   (d20.ma20-2*d20.std20)::real
				   from
				 (select max(a.code) as code,max(a.date) as date,max(close) as close from a where rank=1 ) as d,
			     (select avg(a.close) as ma5 from a where rank<=5) as d5,
				 (select avg(close) as ma10 from a where rank<=10) as d10,
				 (select avg(close) as ma20,stddev_pop(close) as std20 from a where rank<=20) as d20,
				 (select avg(close) as ma30 from a where rank<=30) as d30,
				 (select avg(close) as ma60 from a where rank<=60) as d60;

	-- -------------------------------------------------------------------------------------------
	end loop;
return;
end;
$function$

--=================================================================================================================================================
--=================================================================================================================================================

-- 单个code的MA和boll
CREATE OR REPLACE FUNCTION public.maboll(c text)
 RETURNS TABLE(code text, date date, ma5 real, ma10 real, ma20 real, ma30 real, ma60 real, up20 real, down20 real)
 LANGUAGE plpgsql
 STRICT
AS $function$
declare
begin
    -- -------------------------------------------------------------------------------------------
       return query 
	   with d as (select golang.code,golang.date,golang.close from golang where golang.code=c order by golang.date desc),
	        d5 as (select d.code,d.date,case when count(close) over w=5 then avg(close) over w end as ma5 from d
	                       window w as (order by d.date rows between 4 preceding and current row)),
	        d10 as (select d.date,case when count(close) over w=10 then avg(close) over w end as ma10 from d
	                       window w as (order by d.date rows between 9 preceding and current row)),
	        d20 as (select d.date,case when count(close) over w=20 then avg(close) over w end as ma20,
	                       case when count(close) over w=20 then stddev_pop(close) over w  end as std20
	                       from d
	                       window w as (order by d.date rows between 19 preceding and current row)),
	        d30 as (select d.date,case when count(close) over w=30 then avg(close) over w end as ma30 from d 
	                       window w as (order by d.date rows between 29 preceding and current row)),
	        d60 as (select d.date,case when count(close) over w=60 then avg(close) over w end as ma60 from d 
	                       window w as (order by d.date rows between 59 preceding and current row))
	    select d5.code,
	          d5.date,
	          d5.ma5::real,
	          d10.ma10::real,
	          d20.ma20::real,
	          d30.ma30::real,
	          d60.ma60::real,
	          (d20.ma20+2*d20.std20)::real as up20,
	          (d20.ma20-2*d20.std20)::real as down20
	    from d5 join d10 on d5.date=d10.date
	            join d20 on d5.date=d20.date
	            join d30 on d5.date=d30.date
	            join d60 on d5.date=d60.date;  

	-- -------------------------------------------------------------------------------------------
return;
end;
$function$



--=================================================================================================================================================
--=================================================================================================================================================

-- 存储所有的MA和boll

CREATE OR REPLACE FUNCTION public.maboll_store()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE --定义
x text; 
begin
   create table if not exists maboll(
		code text,
		date date,
		ma5 real,
		ma10 real,
		ma20 real,
		ma30 real,
		ma60 real,
		up20 real,
		down20 real
		);
   drop index if exists maboll_index;
   truncate maboll;
   for x in select stock_code.code from stock_code loop
    -- -------------------------------------------------------------------------------------------
         insert into maboll select * from maboll(x);
	-- -------------------------------------------------------------------------------------------
	end loop;
    create index maboll_index on maboll(code,date);
END
$function$





--=================================================================================================================================================
--=================================================================================================================================================



--任意一天的MA

create or replace function ma(c text,day date,n integer) 
returns table(code text,date date,ma real) 
LANGUAGE plpgsql
STRICT
as
$$
begin
	return query 
	with d as (select golang.code,golang.date,golang.close from golang where golang.code=c and golang.date<=day order by golang.date desc limit n),
	     r as (select d.code, max(d.date) as date,avg(d.close)::real as ma,count(*)::real as num from d group by 1)
	     select r.code,r.date,
	     case when r.num<n then null
	          else r.ma
	     end as ma
	     from r;
return;	
end
$$

--=================================================================================================================================================
--=================================================================================================================================================

--boll交叉预测

with c as (select * from golang where code ='sh600118' order by date desc limit 19 offset 1),
     abc as (select  max(date) as date,
                     (4*20-20^2+2*20-5) as a,
                     ((2*20-10)*sum(c.close)) as b,
                     (4*20*sum(c.close^2)-5*sum(c.close)^2) as c
                     from c)
select date,a,b,c,
       (-b+sqrt(b^2-4*a*c))/(2*a) as x1,
       (-b-sqrt(b^2-4*a*c))/(2*a) as x2
       from abc


--=================================================================================================================================================
--=================================================================================================================================================
	   
	   
--添加计算数据的初步测试

CREATE OR REPLACE FUNCTION test()
RETURNS setof golang
LANGUAGE plpgsql
STRICT
AS 
$$
declare
n integer;
begin
	n := (select max(date)-'2018-10-18' from golang where code='sh600118');
    if n>0  then
    return query select * from golang where code='sh600118' order by date limit 10+n;
    end if;
end;
$$

--=================================================================================================================================================
--=================================================================================================================================================
-- 所有MA BOLL 添加最新

CREATE OR REPLACE FUNCTION public.all_maboll_append()
 RETURNS TABLE(code text, date date, ma5 real, ma10 real, ma20 real, ma30 real, ma60 real, up20 real, down20 real)
 LANGUAGE plpgsql
 STRICT
AS $$
declare
x text;
dday date;
mday date;
begin
   for x in select stock_code.code from stock_code loop
    -- -------------------------------------------------------------------------------------------
       mday := (select max(maboll.date) from maboll where maboll.code=x);
       --mday :='2018-10-15'::date;
       if mday is null then
         mday:=(select min(golang.date) from golang where golang.code=x);
       end if;
	   dday := (select max(golang.date) from golang where golang.code=x); 
       if dday-mday>0  then
          return query 
			   with dd as (select golang.code,golang.date,close from golang where golang.code=x order by golang.date desc limit 60+dday-mday-1),
			        d5 as (select d.code,d.date,case when count(close) over w=5 then avg(close) over w end as ma5 from (select * from dd order by dd.date desc limit 5+dday-mday-1) as d
			                       window w as (order by d.date rows between 4 preceding and current row) order by date desc),
			        d10 as (select d.date,case when count(close) over w=10 then avg(close) over w end as ma10 from (select * from dd order by dd.date desc limit 10+dday-mday-1) as d
			                       window w as (order by d.date rows between 9 preceding and current row) order by date desc),
			        d20 as (select d.date,case when count(close) over w=20 then avg(close) over w end as ma20,
			                       case when count(close) over w=20 then stddev_pop(close) over w  end as std20
			                       from (select * from dd order by dd.date desc limit 20+dday-mday-1) as d
			                       window w as (order by d.date rows between 19 preceding and current row) order by date desc),
			        d30 as (select d.date,case when count(close) over w=30 then avg(close) over w end as ma30 from (select * from dd order by dd.date desc limit 30+dday-mday-1) as d 
			                       window w as (order by d.date rows between 29 preceding and current row) order by date desc),
			        d60 as (select d.date,case when count(close) over w=60 then avg(close) over w end as ma60 from (select * from dd order by dd.date desc limit 60+dday-mday-1) as d 
			                       window w as (order by d.date rows between 59 preceding and current row) order by date desc)
			   select d5.code,
			          d5.date,
			          d5.ma5::real,
			          d10.ma10::real,
			          d20.ma20::real,
			          d30.ma30::real,
			          d60.ma60::real,
			          (d20.ma20+2*d20.std20)::real as up20,
			          (d20.ma20-2*d20.std20)::real as down20
			    from d5 join d10 on d5.date=d10.date
			            join d20 on d5.date=d20.date
			            join d30 on d5.date=d30.date
			            join d60 on d5.date=d60.date
			    where d5.date>mday
			    order by date desc;
        end if;
	-- -------------------------------------------------------------------------------------------
	end loop;
return;
end;
$$

	   
--=================================================================================================================================================
--=================================================================================================================================================
-- 存储所有MA BOLL 添加最新
	   
CREATE OR REPLACE FUNCTION public.all_maboll_store_append()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE --定义

begin
   create table if not exists maboll(
		code text,
		date date,
		ma5 real,
		ma10 real,
		ma20 real,
		ma30 real,
		ma60 real,
		up20 real,
		down20 real
		);
   drop index if exists maboll_index;
   --truncate maboll;
   insert into maboll select * from all_maboll_append(); 
   create index maboll_index on maboll(code,date);
END
$function$



--=================================================================================================================================================
--=================================================================================================================================================
-- 单个code指标的收益率

CREATE OR REPLACE FUNCTION public.rate(c text)
 RETURNS TABLE(code text, date date, tdate date, r real, rco real, rch real, rcl real, rma5 real, rma10 real, rma20 real, rma30 real, rma60 real, rup20 real, rdown20 real, r5ma5 real, r5ma10 real, r5ma20 real, r5ma30 real, r5ma60 real, r5up20 real, r5down20 real, cma5 real, cma10 real, cma20 real, cma30 real, cma60 real, cup20 real, cdown20 real)
 LANGUAGE plpgsql
 STRICT
AS $function$
declare
begin
    -- -------------------------------------------------------------------------------------------
       return query 
	 with d as (select *,rank() over(order by golang.date) as id from golang where golang.code=c),
	     pd as (select d.date,d.close,(d.id+1)as id from d),
	     rd as (select d.code,d.date,
	                  ((d.close-pd.close)/pd.close) as r,
                      ((d.close-d.open)/d.open) as rco,
                      ((d.close-d.high)/d.high) as rch,
                      ((d.close-d.low)/d.low) as rcl	                  
	            from d,pd where d.id=pd.id),
	     mb as (select *,rank() over(order by maboll.date) as id from maboll where maboll.code=c),
	     pmb as (select mb.date,mb.ma5,mb.ma10,mb.ma20,mb.ma30,mb.ma60,mb.up20,mb.down20,(mb.id+1)as id from mb),
	     p5mb as (select mb.date,mb.ma5,mb.ma10,mb.ma20,mb.ma30,mb.ma60,mb.up20,mb.down20,(mb.id+4)as id from mb),
	     rmb as (select mb.code,mb.date,mb.tdate,
	                    ((mb.ma5-pmb.ma5)/pmb.ma5) as rma5,
	                    ((mb.ma10-pmb.ma10)/pmb.ma10) as rma10,
	                    ((mb.ma20-pmb.ma20)/pmb.ma20) as rma20,
	                    ((mb.ma30-pmb.ma30)/pmb.ma30) as rma30,
	                    ((mb.ma60-pmb.ma60)/pmb.ma60) as rma60,
	                    ((mb.up20-pmb.up20)/pmb.up20) as rup20,
	                    ((mb.down20-pmb.down20)/pmb.down20) as rdown20
	             from mb,pmb where mb.id=pmb.id),	             
	     r5mb as (select mb.code,mb.date,mb.tdate,
	                    ((mb.ma5-p5mb.ma5)/p5mb.ma5) as r5ma5,
	                    ((mb.ma10-p5mb.ma10)/p5mb.ma10) as r5ma10,
	                    ((mb.ma20-p5mb.ma20)/p5mb.ma20) as r5ma20,
	                    ((mb.ma30-p5mb.ma30)/p5mb.ma30) as r5ma30,
	                    ((mb.ma60-p5mb.ma60)/p5mb.ma60) as r5ma60,
	                    ((mb.up20-p5mb.up20)/p5mb.up20) as r5up20,
	                    ((mb.down20-p5mb.down20)/p5mb.down20) as r5down20
	             from mb,p5mb where mb.id=p5mb.id),	     
         cmb as (select d.code,d.date,
                ((d.close-mb.ma5)/mb.ma5) as cma5,
                ((d.close-mb.ma10)/mb.ma10) as cma10,
                ((d.close-mb.ma20)/mb.ma20) as cma20,
                ((d.close-mb.ma30)/mb.ma30) as cma30,
                ((d.close-mb.ma60)/mb.ma60) as cma60,
                ((d.close-mb.up20)/mb.up20) as cup20,
                ((d.close-mb.down20)/mb.down20) as cdown20
        from d,mb where d.date=mb.date)
	select 
	   rd.code,
       rd.date,
       rmb.tdate,
       rd.r,
       rd.rco,
       rd.rch,
       rd.rcl,
       rmb.rma5,
       rmb.rma10,
       rmb.rma20,
       rmb.rma30,
       rmb.rma60,
       rmb.rup20,
       rmb.rdown20,
       r5mb.r5ma5,
       r5mb.r5ma10,
       r5mb.r5ma20,
       r5mb.r5ma30,
       r5mb.r5ma60,
       r5mb.r5up20,
       r5mb.r5down20,
       cmb.cma5,
       cmb.cma10,
       cmb.cma20,
       cmb.cma30,
       cmb.cma60,
       cmb.cup20,
       cmb.cdown20
	  from rd,rmb,r5mb,cmb where rd.date=rmb.date and rd.date=cmb.date and rd.date=r5mb.date;
	-- -------------------------------------------------------------------------------------------
return;
end;
$function$
;





 
--=================================================================================================================================================
--=================================================================================================================================================
-- 各种指标的收益率 储存
CREATE OR REPLACE FUNCTION public.rate_store()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE --定义
x text; 
begin
   create table if not exists rate(
                 code text,
				 date date,
				 tdate date, 
				 r real, 
				 rco real,
				 rch real, 
				 rcl real,
				 rma5 real, 
				 rma10 real, 
				 rma20 real, 
				 rma30 real, 
				 rma60 real,
				 rup20 real,
				 rdown20 real,
				 r5ma5 real,
		         r5ma10 real,
		         r5ma20 real,
		         r5ma30 real,
		         r5ma60 real,
		         r5up20 real,
		         r5down20 real,
				 cma5 real,
				 cma10 real,
				 cma20 real,
				 cma30 real,
				 cma60 real,
				 cup20 real,
				 cdown20 real
		);
   drop index if exists rate_index;
   truncate rate;
   for x in select stock_code.code from stock_code loop
    -- -------------------------------------------------------------------------------------------
    insert into rate select * from rate(x);
	-- -------------------------------------------------------------------------------------------
	end loop;
    create index rate_index on rate(code,date,tdate);
END
$function$
;



--=================================================================================================================================================
--=================================================================================================================================================
-- 单个code后向数据
CREATE OR REPLACE FUNCTION public.future(c text)
 RETURNS TABLE(code text, date date, c2 real, c5 real, c10 real, lhh2 real, lll2 real, lhh5 real, lll5 real, lhh10 real, lll10 real)
 LANGUAGE plpgsql
 STRICT
AS $function$
declare
begin
    -- -------------------------------------------------------------------------------------------
       return query 
	with d as (select * from golang where golang.code=c order by golang.date),     
	     d2 as (select d.code,d.date,d.high,d.low,d.close,(rank() over w) as id,
	                       case when count(close) over w=2 then max(high) over w end as lhh2,
	                       case when count(close) over w=2 then min(low) over w end as lll2 
	                       from d
	                       window w as (order by d.date desc rows between 1 preceding and current row) order by date),
	     db as (select d2.code,d2.date,d2.close,d2.id-1 as id from d2),
	     dl as (select d2.code,d2.date,d2.close,d2.id+1 as id from d2),
	     dl5 as (select d2.code,d2.date,d2.close,d2.id+4 as id from d2),
	     dl10 as (select d2.code,d2.date,d2.close,d2.id+9 as id from d2),
	     d5 as (select d.code,d.date,
	                       case when count(close) over w=5 then max(high) over w end as lhh5,
	                       case when count(close) over w=5 then min(low) over w end as lll5 
	                       from d
	                       window w as (order by d.date desc rows between 4 preceding and current row) order by date),
	     d10 as (select d.code,d.date,
	                       case when count(close) over w=10 then max(high) over w end as lhh10,
	                       case when count(close) over w=10 then min(low) over w end as lll10 
	                       from d
	                       window w as (order by d.date desc rows between 9 preceding and current row) order by date)
	     select d2.code,db.date,
	            --d2.high,d2.low,d2.close,
	            dl.close as c2,
	            dl5.close as c5,
	            dl10.close as c10,
	            d2.lhh2,
	            d2.lll2,
	            d5.lhh5,
	            d5.lll5,
	            d10.lhh10,
	            d10.lll10
	   from d2 left join d5 on d2.date=d5.date
	           left join d10 on d2.date=d10.date
	           left join dl on d2.id=dl.id
	           left join dl5 on d2.id=dl5.id
	           left join dl10 on d2.id=dl10.id
	           left join db on d2.id=db.id
	   order by d2.date desc;

	-- -------------------------------------------------------------------------------------------
return;
end;
$function$




 
--=================================================================================================================================================
--=================================================================================================================================================
-- 后向数据 储存
CREATE OR REPLACE FUNCTION public.future_store()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE --定义
x text; 
begin
   create table if not exists future(
		code text,
		date date,
        c2 real,
        c5 real,
        c10 real,
        lhh2 real,
        lll2 real,
        lhh5 real,
        lll5 real,
        lhh10 real,
        lll10 real        
		);
   drop index if exists future_index;
   truncate future;
   for x in select stock_code.code from stock_code loop
    -- -------------------------------------------------------------------------------------------
      insert into future select * from future(x);
	-- -------------------------------------------------------------------------------------------
	end loop;
    create index future_index on future(code,date);
END
$function$





--=================================================================================================================================================
--=================================================================================================================================================
-- 单个code预算各指标的交叉价位
CREATE OR REPLACE FUNCTION public.predict(cc text)
 RETURNS TABLE(date date, code text, c_ma5 real, c_ma10 real, c_ma20 real, c_ma30 real, c_ma60 real, c_down20 real, c_up20 real, ma_5_10 real, ma_5_20 real, ma_5_30 real, ma_5_60 real, ma_10_20 real, ma_10_30 real, ma_10_60 real, ma_20_30 real, ma_20_60 real, ma_30_60 real)
 LANGUAGE plpgsql
 STRICT
AS $function$
declare
begin
    -- -------------------------------------------------------------------------------------------
       return query 
		  		with d4 as (select golang.code,golang.date,rank() over w as id,case when count(close) over w=4 then avg(close) over w end as ma4 from golang where golang.code=cc
		                       window w as (order by golang.date rows between 3 preceding and current row) order by date desc),
		        d9 as (select golang.date,case when count(close) over w=9 then avg(close) over w end as ma9 from golang where golang.code=cc
		                       window w as (order by golang.date rows between 8 preceding and current row) order by date desc),
		        d19 as (select golang.date,case when count(close) over w=19 then avg(close) over w end as ma19,
		                       (4*20-20^2+2*20-5) as a,
		                       case when count(close) over w=19 then ((2*20-10)*(sum(round(close::numeric,2)) over w))  end as b,
		                       case when count(close) over w=19 then (4*20*(sum(round(close::numeric,2)^2) over w) -5*(sum(round(close::numeric,2)) over w)^2)  end as c
		                       from golang where golang.code=cc
		                       window w as (order by golang.date rows between 18 preceding and current row) order by date desc),
		        d29 as (select golang.date,case when count(close) over w=29 then avg(close) over w end as ma29 from golang where golang.code=cc
		                       window w as (order by golang.date rows between 28 preceding and current row) order by date desc),
		        d59 as (select golang.date,case when count(close) over w=59 then avg(close) over w end as ma59 from golang where golang.code=cc
		                       window w as (order by golang.date rows between 58 preceding and current row) order by date desc),
		        d as (select d4.id-1 as id,d4.date from d4)
		   select d.date,
		          d4.code,
		          d4.ma4::real as c_ma5,
		          d9.ma9::real as c_ma10,
		          d19.ma19::real as c_ma20,
		          d29.ma29::real as c_ma30,
		          d59.ma59::real as c_ma60,
		          ((-d19.b+sqrt(d19.b^2-4*d19.a*d19.c))/(2*d19.a))::real as c_down20,
		          ((-d19.b-sqrt(d19.b^2-4*d19.a*d19.c))/(2*d19.a))::real as c_up20,
		          ((5*(10-1)*d9.ma9-10*(5-1)*d4.ma4)/(10-5))::real as ma_5_10,
		          ((5*(20-1)*d19.ma19-20*(5-1)*d4.ma4)/(20-5))::real as ma_5_20,
		          ((5*(30-1)*d29.ma29-30*(5-1)*d4.ma4)/(30-5))::real as ma_5_30,
		          ((5*(60-1)*d59.ma59-60*(5-1)*d4.ma4)/(60-5))::real as ma_5_60,
		          ((10*(20-1)*d19.ma19-20*(10-1)*d9.ma9)/(20-10))::real as ma_10_20,
		          ((10*(30-1)*d29.ma29-30*(10-1)*d9.ma9)/(30-10))::real as ma_10_30,
		          ((10*(60-1)*d59.ma59-60*(10-1)*d9.ma9)/(60-10))::real as ma_10_60,
		          ((20*(30-1)*d29.ma29-30*(20-1)*d19.ma19)/(30-20))::real as ma_20_30,
		          ((20*(60-1)*d59.ma59-60*(20-1)*d19.ma19)/(60-20))::real as ma_20_60,
		          ((30*(60-1)*d59.ma59-60*(30-1)*d29.ma29)/(60-30))::real as ma_30_60
		    from d4 join d9 on d4.date=d9.date
		            join d19 on d4.date=d19.date
		            join d29 on d4.date=d29.date
		            join d59 on d4.date=d59.date
		            join d on d4.id=d.id;

	-- -------------------------------------------------------------------------------------------
return;
end;
$function$



--=================================================================================================================================================
--=================================================================================================================================================
-- 储存预算各指标的交叉价位
CREATE OR REPLACE FUNCTION public.predict_store()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE --定义
x text; 
begin
   create table if not exists predict(
        date date,
		code text,
		c_ma5 real,
	    c_ma10 real,
	    c_ma20 real,
	    c_ma30 real,
	    c_ma60 real,
	    c_down20 real,
	    c_up20 real,
	    ma_5_10 real,
	    ma_5_20 real,
	    ma_5_30 real,
	    ma_5_60 real,
	    ma_10_20 real,
	    ma_10_30 real,
	    ma_10_60 real,
	    ma_20_30 real,
	    ma_20_60 real,
	    ma_30_60  real    
		);
   drop index if exists predict_index;
   truncate predict;
   for x in select stock_code.code from stock_code loop
   --raise notice 'code=:%',x;
    -- -------------------------------------------------------------------------------------------
     insert into predict select * from predict(x);
	-- -------------------------------------------------------------------------------------------
	end loop;
    create index predict_index on predict(code,date);
END
$function$



--=================================================================================================================================================
--=================================================================================================================================================
-- 斜率聚合函数ssfun
CREATE OR REPLACE FUNCTION public.lsp_sf(arr real[], y real)
 RETURNS real[]
 LANGUAGE plpgsql
AS $function$
  declare
  sxy real;
  sx real;
  sy real;
  x real;
  sx2 real;
  r real[];
  begin
	--raise notice 'aa:%,s:%',arr,y;
	if y is null then
	   r='{}';
	else
		if (array_length(arr,1) is null) then
	       x=1;
	       sxy=x*y;
	       sx=x;
	       sy=y;
	       sx2=x*x;
		else
		    if y is null then
		       x=null;
		    else
		       x=arr[1]+1;   
		    end if;
		    sxy=arr[2]+x*y;
		    sx=arr[3]+x;
		    sy=arr[4]+y;
		    sx2=arr[5]+x*x;
		end if;
	    r=array[x,sxy,sx,sy,sx2];
    end if ;
RETURN r;
END;
$function$

 
 --=================================================================================================================================================
--=================================================================================================================================================
-- 斜率聚合函数FINALFUNC
 CREATE FUNCTION "public"."lsp_ff"(arr real[])
  RETURNS real AS $BODY$
  declare
  n real;
  sxy real;
  sx real;
  sy real;
  y real;
  sx2 real;
  r real;
begin
  if (array_length(arr,1) is null) then	
     r=null;
  else
    n=arr[1];
    sxy=arr[2];
    sx=arr[3];
    sy=arr[4];
    sx2=arr[5];
  --raise notice 'n:%,sxy:%,sx:%,sy:%,sx2:%',n,sxy,sx,sy,sx2;
	  if (n*sx2-sx*sx)=0 then
	     r=0;
	  else
	     r=(n*sxy-sx*sy)/(n*sx2-sx*sx);
	  end if;
   end if;
RETURN r;
END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE COST 100;

 --=================================================================================================================================================
--=================================================================================================================================================
-- 斜率聚合函数AGGREGATE
CREATE AGGREGATE lsp(real) (
    SFUNC = lsp_sf,
    STYPE = real[],
      FINALFUNC = lsp_ff,
      initcond='{}'
);

 --=================================================================================================================================================
--=================================================================================================================================================
-- 单一code 的rate表斜率计算
CREATE OR REPLACE FUNCTION public.rate_slope(c text, n integer)
 RETURNS TABLE(code text, date date, tdate date, lr real, lrco real, lrch real, lrcl real, lrma5 real, lrma10 real, lrma20 real, lrma30 real, lrma60 real, lrup20 real, lrdown20 real, lcma5 real, lcma10 real, lcma20 real, lcma30 real, lcma60 real, lcup20 real, lcdown20 real)
 LANGUAGE plpgsql
 STRICT
AS $function$
declare
begin
    -- -------------------------------------------------------------------------------------------
    return query 
    with d as (select *,(rank() over w) as id from rate where rate.code=c
               window w as (order by rate.date)),
         td as (select d.date as tdate,(d.id-1) as id from d)
	select d.code,d.date,td.tdate,
	           ((regr_slope(d.r,d.id::real/100) over w)::real) as lr,
	           ((regr_slope(d.rco,d.id::real/100)over w)::real) as lrco,
	           ((regr_slope(d.rch,d.id::real/100)over w)::real) as lrch,
	           ((regr_slope(d.rcl,d.id::real/100)over w)::real) as lrcl,
	           ((regr_slope(d.rma5,d.id::real/100)over w)::real) as lrma5,
	           ((regr_slope(d.rma10,d.id::real/100)over w)::real) as lrma10,
	           ((regr_slope(d.rma20,d.id::real/100)over w)::real) as lrma20,
	           ((regr_slope(d.rma30,d.id::real/100)over w)::real) as lrma30,
	           ((regr_slope(d.rma60,d.id::real/100)over w)::real) as lrma60,
	           ((regr_slope(d.rup20,d.id::real/100)over w)::real) as lrup20,
	           ((regr_slope(d.rdown20,d.id::real/100)over w)::real) as lrdown20,
	           ((regr_slope(d.cma5,d.id::real/100)over w)::real) as lcma5,
	           ((regr_slope(d.cma10,d.id::real/100)over w)::real) as lcma10,
	           ((regr_slope(d.cma20,d.id::real/100)over w)::real) as lcma20,
	           ((regr_slope(d.cma30,d.id::real/100)over w)::real) as lcma30,
	           ((regr_slope(d.cma60,d.id::real/100)over w)::real) as lcma60,
	           ((regr_slope(d.cup20,d.id::real/100)over w)::real) as lcup20,
	           ((regr_slope(d.cdown20,d.id::real/100)over w)::real) as lcdown20
     from d left join td on d.id=td.id
     window w as (order by d.date rows between n-1 preceding and current row);

	-- -------------------------------------------------------------------------------------------
return;
end;
$function$


 --=================================================================================================================================================
--=================================================================================================================================================
-- 储存所有code 的rate表斜率计算
CREATE OR REPLACE FUNCTION srate_store()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE --定义
x text; 
begin
   create table if not exists srate(
                code text,
		        date date,
		        tdate date,
				lr real,
				lrco real,
				lrch real,
				lrcl real,
				lrma5 real,
				lrma10 real,
				lrma20 real,
				lrma30 real,
				lrma60 real,
				lrup20 real,
				lrdown20 real,
			    lcma5 real,
		        lcma10 real,
		        lcma20 real,
		        lcma30 real,
		        lcma60 real,
		        lcup20 real,
		        lcdown20 real
		);
   drop index if exists srate_index;
   truncate srate;
   for x in select stock_code.code from stock_code loop
    -- -------------------------------------------------------------------------------------------
         insert into srate select * from rate_slope(x,5);
	-- -------------------------------------------------------------------------------------------
	end loop;
    create index srate_index on srate(code,date,tdate);
END
$function$
 --=================================================================================================================================================
--=================================================================================================================================================
-- 最近N条相关预测所需要的MA和BOLL数据

CREATE OR REPLACE FUNCTION public.maboll_recent(cc text, n integer)
 RETURNS TABLE(code text, 
 date date, 
 o real, 
 c real, 
 h real, 
 l real, 
 v real,
 ma5 real,
 ma10 real,
 ma20 real,
 ma30 real, 
 ma60 real,
 up20 real, 
 down20 real,
 c_ma5 real, 
 c_ma10 real, 
 c_ma20 real,
 c_down20 real,
 c_up20 real,
 c_ma30 real, 
 c_ma60 real, 
 ma_5_10 real, 
 ma_5_20 real, 
 ma_5_30 real, 
 ma_5_60 real,
 ma_10_20 real,
 ma_10_30 real, 
 ma_10_60 real, 
 ma_20_30 real, 
 ma_20_60 real, 
 ma_30_60 real)
 LANGUAGE plpgsql
 STRICT
AS $function$
declare
begin
    -- -------------------------------------------------------------------------------------------
       return query 
	   with d as (select * from golang where golang.code=cc order by golang.date desc limit n+59),
	        d4 as (select d.code,case when count(d.close) over w=4 then avg(d.close) over w end as ma4 from d
		                   window w as (order by d.date rows between 3 preceding and current row) order by date desc),
	        d5 as (select d.*,case when count(d.close) over w=5 then avg(d.close) over w end as ma5 from d
	                       window w as (order by d.date rows between 4 preceding and current row)),
	        d9 as (select d.date,case when count(close) over w=9 then avg(close) over w end as ma9 from d
		                   window w as (order by d.date rows between 8 preceding and current row) order by date desc),
	        d10 as (select d.date,case when count(close) over w=10 then avg(close) over w end as ma10 from d
	                       window w as (order by d.date rows between 9 preceding and current row)),
	        d19 as (select d.date,case when count(close) over w=19 then avg(close) over w end as ma19,
	                       (4*20-20^2+2*20-5) as a,
	                       case when count(close) over w=19 then ((2*20-10)*(sum(round(close::numeric,2)) over w))  end as b,
	                       case when count(close) over w=19 then (4*20*(sum(round(close::numeric,2)^2) over w) -5*(sum(round(close::numeric,2)) over w)^2)  end as c 
	                       from d
	                       window w as (order by d.date rows between 18 preceding and current row) order by date desc),
	        d20 as (select d.date,case when count(close) over w=20 then avg(close) over w end as ma20,
	                       case when count(close) over w=20 then stddev_pop(close) over w  end as std20
	                       from d
	                       window w as (order by d.date rows between 19 preceding and current row)),	        
	        d29 as (select d.date,case when count(close) over w=29 then avg(close) over w end as ma29 from d
                           window w as (order by d.date rows between 28 preceding and current row) order by date desc),               
	        d30 as (select d.date,case when count(close) over w=30 then avg(close) over w end as ma30 from d 
	                       window w as (order by d.date rows between 29 preceding and current row)),
	        d59 as (select d.date,case when count(close) over w=59 then avg(close) over w end as ma59 from d
                           window w as (order by d.date rows between 58 preceding and current row) order by date desc),
	        d60 as (select d.date,case when count(close) over w=60 then avg(close) over w end as ma60 from d 
	                       window w as (order by d.date rows between 59 preceding and current row))
	    select d5.code,
	          d5.date,
	          d5.open::real,
	          d5.close::real,
	          d5.high::real,
	          d5.low::real,
	          d5.volume::real,
	          d5.ma5::real,
	          d10.ma10::real,
	          d20.ma20::real,
	          d30.ma30::real,
	          d60.ma60::real,
	          (d20.ma20+2*d20.std20)::real as up20,
	          (d20.ma20-2*d20.std20)::real as down20,
	          d4.ma4::real as c_ma5,
	          d9.ma9::real as c_ma10,
	          d19.ma19::real as c_ma20,
	          ((-d19.b+sqrt(d19.b^2-4*d19.a*d19.c))/(2*d19.a))::real as c_down20,
		      ((-d19.b-sqrt(d19.b^2-4*d19.a*d19.c))/(2*d19.a))::real as c_up20,
		      d29.ma29::real as c_ma30,
		      d59.ma59::real as c_ma60,
	      	  ((5*(10-1)*d9.ma9-10*(5-1)*d4.ma4)/(10-5))::real as ma_5_10,
	          ((5*(20-1)*d19.ma19-20*(5-1)*d4.ma4)/(20-5))::real as ma_5_20,
	          ((5*(30-1)*d29.ma29-30*(5-1)*d4.ma4)/(30-5))::real as ma_5_30,
	          ((5*(60-1)*d59.ma59-60*(5-1)*d4.ma4)/(60-5))::real as ma_5_60,
	          ((10*(20-1)*d19.ma19-20*(10-1)*d9.ma9)/(20-10))::real as ma_10_20,
	          ((10*(30-1)*d29.ma29-30*(10-1)*d9.ma9)/(30-10))::real as ma_10_30,
	          ((10*(60-1)*d59.ma59-60*(10-1)*d9.ma9)/(60-10))::real as ma_10_60,
	          ((20*(30-1)*d29.ma29-30*(20-1)*d19.ma19)/(30-20))::real as ma_20_30,
	          ((20*(60-1)*d59.ma59-60*(20-1)*d19.ma19)/(60-20))::real as ma_20_60,
	          ((30*(60-1)*d59.ma59-60*(30-1)*d29.ma29)/(60-30))::real as ma_30_60
	    from d5 join d10 on d5.date=d10.date
	            join d20 on d5.date=d20.date
	            join d30 on d5.date=d30.date
	            join d60 on d5.date=d60.date
	            join d4 on d5.date=d4.date
	            join d9 on d5.date=d9.date
	            join d19 on d5.date=d19.date
	            join d29 on d5.date=d29.date
	            join d59 on d5.date=d59.date
         order by date desc
         limit n;

	-- -------------------------------------------------------------------------------------------
return;
end;
$function$
;


 --=================================================================================================================================================
--=================================================================================================================================================
-- 循环--最近N条相关预测所需要的MA和BOLL数据

CREATE OR REPLACE FUNCTION public.maboll_recent_loop(n integer)
 RETURNS TABLE(
 rup real,
 rdown real,
 code text, 
 date date, 
 o real, 
 c real, 
 h real, 
 l real, 
 v real,
 ma5 real,
 ma10 real,
 ma20 real,
 ma30 real, 
 ma60 real,
 up20 real, 
 down20 real,
 c_ma5 real, 
 c_ma10 real, 
 c_ma20 real,
 c_down20 real,
 c_up20 real,
 c_ma30 real, 
 c_ma60 real, 
 ma_5_10 real, 
 ma_5_20 real, 
 ma_5_30 real, 
 ma_5_60 real,
 ma_10_20 real,
 ma_10_30 real, 
 ma_10_60 real, 
 ma_20_30 real, 
 ma_20_60 real, 
 ma_30_60 real)
 LANGUAGE plpgsql
 STRICT
AS $function$
declare
x text;
begin
	for x in select stock_code.code from stock_code loop
    -- -------------------------------------------------------------------------------------------
       return query 
       with  d as (select * from maboll_recent(x,n) order by date),
             df as (select d.up20,d.down20 from d order by d.date limit 1),
             dt as (select * from d order by d.date desc limit 1)
             select (dt.up20-df.up20)/df.up20 as rup20,
                    (dt.down20-df.down20)/df.down20 as rdown20,
                    dt.*
                   from dt,df;
	-- -------------------------------------------------------------------------------------------
end loop;
return;
end;
$function$
;

-- =======================================================================================================================
-- =======================================================================================================================
-- 存储循环--最近N条相关预测所需要的MA和BOLL数据

CREATE OR REPLACE FUNCTION public.maboll_recent_store(n integer)
  RETURNS void
  LANGUAGE plpgsql
  AS $function$
  DECLARE --定义
  x text;
  begin
	drop table if exists recent;
	create table if not exists recent(
		 rup real,
		 rdown real,
		 code text, 
		 date date, 
		 o real, 
		 c real, 
		 h real, 
		 l real, 
		 v real,
		 ma5 real,
		 ma10 real,
		 ma20 real,
		 ma30 real, 
		 ma60 real,
		 up20 real, 
		 down20 real,
		 c_ma5 real, 
		 c_ma10 real, 
		 c_ma20 real,
		 c_down20 real,
		 c_up20 real,
		 c_ma30 real, 
		 c_ma60 real, 
		 ma_5_10 real, 
		 ma_5_20 real, 
		 ma_5_30 real, 
		 ma_5_60 real,
		 ma_10_20 real,
		 ma_10_30 real, 
		 ma_10_60 real, 
		 ma_20_30 real, 
		 ma_20_60 real, 
		 ma_30_60 real);
	--drop index if exists recent_index;
    --truncate recent;
    for x in select stock_code.code from stock_code loop
    -- -------------------------------------------------------------------------------------------
       with  d as (select * from maboll_recent(x,n) order by date),
             df as (select d.up20,d.down20 from d order by d.date limit 1),
             dt as (select * from d order by d.date desc limit 1)
        insert into recent (select (dt.up20-df.up20)/df.up20 as rup20,
                    (dt.down20-df.down20)/df.down20 as rdown20,
                    dt.*
             from dt,df);
	-- -------------------------------------------------------------------------------------------
    end loop;
	create index recent_index on recent(code,date);	 
  end
  $function$
  ;

-- =======================================================================================================================
-- =======================================================================================================================
-- 寻找顶点(包含顶点左右尺度)

CREATE OR REPLACE FUNCTION public.findtop(c text)
 RETURNS TABLE(code text, date date,high real,low real,top int,datel date,rleft real,dateR date,rright real)
 LANGUAGE plpgsql
 STRICT
AS $function$
declare
  base RECORD;
  pos RECORD;
  dir int;
  id int;  
  st date;
  ed date; 
  dateL date;
  baseL real;
  dateR date;
  baseR real;
begin
    -- -------------------------------------------------------------------------------------------
  dir=0;
  id=1;
  select golang.date into st from golang where golang.code=c order by golang.date limit 1;
  select golang.date into ed from golang where golang.code=c order by golang.date desc limit 1;
  for pos in select golang.date,golang.code,golang.high,golang.low from golang where golang.code=c order by golang.date loop
       if id=1 then
          base=pos;
          id=id+1;
          continue;
       end if;
       
       if pos.high<=base.high and pos.low>=base.low then
         return query select pos.code,pos.date,pos.high,pos.low,0,null::date,null::real,null::date,null::real; 
         continue;
       end if;
      
      if pos.high>=base.high and pos.low<=base.low then
         return query select base.code,base.date,base.high,base.low,0,null::date,null::real,null::date,null::real;
         base=pos;
         continue;
       end if;

      if dir=0 and pos.high>base.high and pos.low>=base.low then
         return query select base.code,base.date,base.high,base.low,0,null::date,null::real,null::date,null::real;
         base=pos;
         dir=1;
         continue;
      end if;
     
      if dir=0 and pos.high<=base.high and pos.low<base.low then
         return query select base.code,base.date,base.high,base.low,0,null::date,null::real,null::date,null::real;
         base=pos;
         dir=-1;
         continue;       
      end if;
     
      if dir=1 and pos.high>base.high and pos.low>=base.low then
         return query select base.code,base.date,base.high,base.low,0,null::date,null::real,null::date,null::real;
         base=pos;
         continue;     
      end if;
     
      if dir=1 and pos.high<=base.high and pos.low<base.low then
        -- ----------------------------------------------------
         select golang.date into dateL from golang where golang.code=c and golang.high>base.high and golang.date<base.date order by golang.date desc limit 1;
         select golang.date into dateR from golang where golang.code=c and golang.high>base.high and golang.date>base.date order by golang.date limit 1;
        if dateL is null then
            dateL=st;
        end if;
        if dateR is null then 
            dateR=ed;
        end if;
        select min(golang.low) into baseL from golang where golang.code=c and golang.date>dateL and golang.date<base.date;
        select min(golang.low) into baseR from golang where golang.code=c and golang.date<dateR and golang.date>base.date;
        -- ----------------------------------------------------
         return query select base.code,base.date,base.high,base.low,1,dateL,((base.high-baseL)*100/baseL)::real,dateR,((base.high-baseR)*100/base.high)::real;
         base=pos;
         dir=-1;
         continue;     
      end if;
     
      if dir=-1 and pos.high>base.high and pos.low>=base.low then
         -- ----------------------------------------------------
        select golang.date into dateL from golang where golang.code=c and golang.low<base.low and golang.date<base.date order by golang.date desc limit 1;
        select golang.date into dateR from golang where golang.code=c and golang.low<base.low and golang.date>base.date order by golang.date limit 1;
        if dateL is null then
            dateL=st;
         end if;
        if dateR is null then
            dateR=ed;
         end if;
        select max(golang.high) into baseL from golang where golang.code=c and golang.date>dateL and golang.date<base.date;
        select max(golang.high) into baseR from golang where golang.code=c and golang.date<dateR and golang.date>base.date;
         -- ----------------------------------------------------
         return query select base.code,base.date,base.high,base.low,-1,dateL,((base.low-baseL)*100/baseL)::real,dateR,((base.low-baseR)*100/base.low)::real;
         base=pos;
         dir=1;
         continue;              
      end if;
     
      if dir=-1 and pos.high<=base.high and pos.low<base.low then
         return query select base.code,base.date,base.high,base.low,0,null::date,null::real,null::date,null::real;
         base=pos;
         continue;           
      end if;
      
   end loop;

	-- -------------------------------------------------------------------------------------------
return;
end;
$function$
;
