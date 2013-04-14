/* 

  PL/SQL oracle script that processes incoming stock bid and sell orders,
    generates a table of quotes that is used to match the bid and sell 
    orders with one another, and results in a table Trades that reflects 
    how the bid and sell orders were actually processed and a Quotes table
    that reflects the actual stock quotes that were available and used.

  The script takes an incoming order, buy or sell, then checks whether there
    is a pending sell order if the current order is a buy, or if there is a
    pending buy order if the current order is a sell. If there is no pending 
    order to fulfill the current order, then the price of the current order is 
    compared against all pending prices to see whether this new price is better
    and can be used as the current quote.  

        For example:

            If there is a pending Buy order for $109.00 and another Buy order
            comes in at $109.50, then that new buy order will become the current
            Buy quote, and the lower one will move back on the queue. If the new 
            Buy order is for $108.50, then the pending Buy order for $109.00 still
            remains as the current buy quote and the lower one will move to the 
            back of the queue. Basically, anytime there is a new Buy order and
            no Sell orders to fulfill it, the highest price Buy order becomes the 
            current Buy quote. The same is done for Sell orders with the only 
            change being that we are looking for the lowest Sell price.

*/


declare

buy_quantity number;
sell_quantity number;
count_ordr number;
var_tdate date;
var_symbol varchar2(20); 
var_tim number; 
var_seq number;
var_oid varchar2(20);
var_side varchar2(20);
var_price number;
var_qty number;
i integer;
trade_id number;
max_bid number;
max_bid_quantity number;
quote_number number;
bid_quotes number;
max_bid_quote number;
max_bid_oid varchar2(50);
sell_quotes number;
min_sell number;
min_sell_quantity number;
min_sell_seq number;
min_sell_oid varchar2(55);
max_bid_seq number;
current_symbol varchar2(55);
max_bid_date date;
min_sell_date date;
max_bid_symbol varchar2(55);
min_sell_symbol varchar2(55);
max_bid_time number;
min_sell_time number;
   
type cursor_variable is ref cursor;
v_myCursor cursor_variable;


Begin


i := 1;
buy_quantity := 0;
sell_quantity := 0;
trade_id := 1;
max_bid := 0;
max_bid_quantity := 0;
quote_number := 1;
bid_quotes := 0;
max_bid_quote := 0;
sell_quotes := 0;
min_sell := 0;
min_sell_seq := 0;
min_sell_quantity := 0;
max_bid_seq := 0;


select count(*) into count_ordr from ordr; 

select symbol into current_symbol from (
select symbol 
    from ordr
    order by symbol, tim asc)
    where rownum = 1;

  loop
  
  open v_myCursor for
  select tdate, symbol, tim, seq, ooid, side, price, qty
    from (
      select a.*, rownum as rn 
        from ( 
              select * 
                from ordr
                order by symbol, tim asc
              ) a
        ) b
    where rn = i;
    
    fetch v_myCursor into var_tdate, var_symbol, var_tim, var_seq, var_oid, var_side, var_price, var_qty;
    exit when v_myCursor%NOTFOUND;
    
    if var_symbol <> current_symbol then
      buy_quantity := 0;
      sell_quantity := 0;
      bid_quotes := 0;
      sell_quotes := 0;
      current_symbol := var_symbol;
    end if;

    
    if var_side = 'B' 
      then 
        buy_quantity := buy_quantity + var_qty;
      else 
        sell_quantity := sell_quantity + var_qty;
    end if;
    
    if var_side = 'B'
      then
      
        if sell_quantity = 0 then
          insert into quotes (tdate, symbol, tim, seq, bid, bidqty, ooid) 
            values (var_tdate, var_symbol, var_tim, var_seq, var_price, var_qty,var_oid);
          bid_quotes := bid_quotes + 1;
        end if;
        
        if sell_quotes > 0 and var_qty > 0 then
        
         loop
        
        select ofr, seq, ofrqty, ooid, tdate, symbol, tim into min_sell,min_sell_seq, min_sell_quantity, min_sell_oid, min_sell_date, min_sell_symbol, min_sell_time  
            from (select ofr, seq, ofrqty, ooid, tdate, symbol, tim from quotes where ofr is not null and symbol = current_symbol order by ofr asc) where rownum <= 1;
        insert into quotes2 (seq, ofr, ofrqty, tdate, symbol, tim, quotenumber) 
              values (min_sell_seq, min_sell, min_sell_quantity, min_sell_date, min_sell_symbol, min_sell_time, quote_number);
        quote_number := quote_number + 1;
        
          if var_qty < min_sell_quantity then
            update quotes set ofrqty = (min_sell_quantity - var_qty), tim = var_tim where seq = min_sell_seq;
            insert into trades (tdate, symbol, tim, seq, price, qty, tid, ooid) 
              values (var_tdate, var_symbol, var_tim, var_seq, min_sell, var_qty, trade_id, var_oid );
            insert into trades (tdate, symbol, tim, seq, price, qty, tid, ooid) 
              values (var_tdate, var_symbol, var_tim, var_seq, min_sell, var_qty, trade_id, min_sell_oid);     
            trade_id := trade_id + 1;
            buy_quantity := buy_quantity - var_qty;
            sell_quantity := sell_quantity - var_qty;
            exit;
          end if;
        
          
          if var_qty = min_sell_quantity then
            delete from quotes where seq = min_sell_seq;
            sell_quotes := sell_quotes - 1;
            insert into trades (tdate, symbol, tim, seq, price, qty, tid, ooid) 
              values (var_tdate, var_symbol, var_tim, var_seq, min_sell, var_qty, trade_id, var_oid );
            insert into trades (tdate, symbol, tim, seq, price, qty, tid, ooid) 
              values (var_tdate, var_symbol, var_tim, var_seq, min_sell, var_qty, trade_id, min_sell_oid);     
            trade_id := trade_id + 1;
            buy_quantity := buy_quantity - var_qty;
            sell_quantity := sell_quantity - var_qty;
            min_sell := 0;
            min_sell_quantity := 0;
            min_sell_seq := 0;
            min_sell_oid := 0;
            exit; 
          end if;
            
          
          if var_qty > min_sell_quantity then
            delete from quotes where seq = min_sell_seq;
            sell_quotes := sell_quotes - 1;
            insert into trades (tdate, symbol, tim, seq, price, qty, tid, ooid) 
              values (var_tdate, var_symbol, var_tim, var_seq, min_sell, min_sell_quantity, trade_id, var_oid );
            insert into trades (tdate, symbol, tim, seq, price, qty, tid, ooid) 
              values (var_tdate, var_symbol, var_tim, var_seq, min_sell, min_sell_quantity, trade_id, min_sell_oid);     
            trade_id := trade_id + 1;
            buy_quantity := buy_quantity - min_sell_quantity;
            sell_quantity := sell_quantity - min_sell_quantity;
            var_qty := var_qty - min_sell_quantity;
          end if;
          
          if sell_quotes = 0 then
          insert into quotes (tdate, symbol, tim, seq, bid, bidqty, ooid) 
            values (var_tdate, var_symbol, var_tim, var_seq, var_price, var_qty, var_oid);
          bid_quotes := bid_quotes + 1;
        end if;
          
          exit when sell_quotes = 0;
        
        end loop;
        
        end if;
      
      end if;
      
      
      
      if var_side = 'S'
      then
      
        if buy_quantity = 0 then
          insert into quotes (tdate, symbol, tim, seq, ofr, ofrqty, ooid) 
            values (var_tdate, var_symbol, var_tim, var_seq, var_price, var_qty, var_oid);
          sell_quotes := sell_quotes + 1;
        end if;
        
        if bid_quotes > 0 and var_qty > 0 then
        
        loop
        
        select bid, seq, bidqty, ooid, tdate, symbol, tim into max_bid, max_bid_seq, max_bid_quantity, max_bid_oid, max_bid_date, max_bid_symbol, max_bid_time  
            from (select bid, seq, bidqty, ooid, tdate, symbol, tim from quotes where bid is not null and symbol = current_symbol order by bid desc) where rownum <= 1;
        insert into quotes2 (seq, bid, bidqty, tdate, symbol, tim, quotenumber) 
              values (max_bid_seq, max_bid, max_bid_quantity, max_bid_date, max_bid_symbol, max_bid_time, quote_number);
        quote_number := quote_number + 1;
        
        dbms_output.put_line('current symbol is ' || current_symbol); 
        
        
          if var_qty < max_bid_quantity then
            update quotes set bidqty = (max_bid_quantity - var_qty), tim = var_tim where seq = max_bid_seq;
            insert into trades (tdate, symbol, tim, seq, price, qty, tid, ooid) 
              values (var_tdate, var_symbol, var_tim, var_seq, max_bid, var_qty, trade_id, var_oid );
            insert into trades (tdate, symbol, tim, seq, price, qty, tid, ooid) 
              values (var_tdate, var_symbol, var_tim, var_seq, max_bid, var_qty, trade_id, max_bid_oid);     
            trade_id := trade_id + 1;
            buy_quantity := buy_quantity - var_qty;
            sell_quantity := sell_quantity - var_qty;
            exit;
            
          end if;
          
          if var_qty = max_bid_quantity then
            delete from quotes where seq = max_bid_seq;
            bid_quotes := bid_quotes - 1;
            insert into trades (tdate, symbol, tim, seq, price, qty, tid, ooid) 
              values (var_tdate, var_symbol, var_tim, var_seq, max_bid, var_qty, trade_id, var_oid );
            insert into trades (tdate, symbol, tim, seq, price, qty, tid, ooid) 
              values (var_tdate, var_symbol, var_tim, var_seq, max_bid, var_qty, trade_id, max_bid_oid);     
            trade_id := trade_id + 1;
            buy_quantity := buy_quantity - var_qty;
            sell_quantity := sell_quantity - var_qty;
            max_bid := 0;
            max_bid_quantity := 0;
            max_bid_seq := 0;
            max_bid_oid := 0;
            exit;
          end if;
          
          if var_qty > max_bid_quantity then
            delete from quotes where seq = max_bid_seq;
            bid_quotes := bid_quotes - 1;
            insert into trades (tdate, symbol, tim, seq, price, qty, tid, ooid) 
              values (var_tdate, var_symbol, var_tim, var_seq, max_bid, max_bid_quantity, trade_id, var_oid );
            insert into trades (tdate, symbol, tim, seq, price, qty, tid, ooid) 
              values (var_tdate, var_symbol, var_tim, var_seq, max_bid, max_bid_quantity, trade_id, max_bid_oid);     
            trade_id := trade_id + 1;
            buy_quantity := buy_quantity - max_bid_quantity;
            sell_quantity := sell_quantity - max_bid_quantity;
            var_qty := var_qty - max_bid_quantity;
          end if;
          
          if bid_quotes = 0 then
           insert into quotes (tdate, symbol, tim, seq, ofr, ofrqty, ooid) 
            values (var_tdate, var_symbol, var_tim, var_seq, var_price, var_qty, var_oid);
          sell_quotes := sell_quotes + 1;
        end if;
          
          exit when bid_quotes = 0;
        
        end loop;
        
        end if;
        
       end if;
       
      i := i + 1;
      
      exit when i > count_ordr;
      
      end loop;
      
      dbms_output.put_line('the end ' || i); 
       
       end;