CREATE OR REPLACE FUNCTION public.find(cc text)
 RETURNS TABLE(code text, date date, rchannl real, low real, c_down20 real, rlhh2 real, rlhh5 real, rlhh10 real, rc2 real, rc5 real, rc10 real)
 LANGUAGE plpgsql
 STRICT
AS $function$
declare
begin
    -- -------------------------------------------------------------------------------------------
       return query 
		with m as (select lag(maboll.date,-1) over(order by maboll.date) as tdate,maboll.code,(up20-down20)/down20*100 as rchannl from maboll where maboll.code=cc),  
		     g as (select golang.date,golang.code,golang.low from golang where golang.code=cc),
		     c as (select predict.date,predict.code,predict.c_down20 from predict where predict.code=cc),
		     f as (select future.date,future.code,lhh2,lhh5,lhh10,c2,c5,c10 from future where future.code=cc)
		select g.code,g.date,
		       m.rchannl:: real,
		       g.low::real,
		       c.c_down20::real,
		       (f.lhh2-c.c_down20)/c.c_down20*100::real as rlhh2,
		       (f.lhh5-c.c_down20)/c.c_down20*100::real as rlhh5,
		       (f.lhh10-c.c_down20)/c.c_down20*100::real as rlhh10,
		       (f.c2-c.c_down20)/c.c_down20*100::real as rc2,
		       (f.c5-c.c_down20)/c.c_down20*100::real as rc5,
		       (f.c10-c.c_down20)/c.c_down20*100::real as rc10
		      from m,g,c,f
		      where m.tdate=g.date and
		            c.date=g.date and
		            f.date=g.date and
		            m.rchannl<8  and
		            g.low<c.c_down20
		      order by date desc;

	-- -------------------------------------------------------------------------------------------
return;
end;
$function$
;
