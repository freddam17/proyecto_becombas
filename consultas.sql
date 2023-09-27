select
	tx.co_bloque::integer,
	fbscript(tx.co_bloque) as fbscript
from 
(select '42' as co_bloque union
select '46' union
select '49' union
select '51' union
select '53' union
select '62' union
select '63' union
select '64' union
select '65' union
select '73' union
select '74' union
select '75' union
select '76' union
select '77' union
select '83' union
select '88' union
select '89' union
select '90' union
select '92' union
select '97' union
select '99' union
select '143' union
select '144' union
select '145' union
select '146' union
select '151' union
select '158' union
select '159' union
select '160' union
select '162' union
select '165' union
select '166' union
select '171' union
select '187' union
select '203' union
select '208' union
select '215' union
select '221' union
select '222' union
select '223' union
select '224' union
select '228' union
select '255'
) tx
order by 1

select
	tx.co_bloque::integer,
	'-- Bloque ' || tx.co_bloque || ' ' || vb.tx_funwf2
from 
(select '42' as co_bloque union
select '46' union
select '49' union
select '51' union
select '53' union
select '62' union
select '63' union
select '64' union
select '65' union
select '73' union
select '74' union
select '75' union
select '76' union
select '77' union
select '83' union
select '88' union
select '89' union
select '90' union
select '92' union
select '97' union
select '99' union
select '143' union
select '144' union
select '145' union
select '146' union
select '151' union
select '158' union
select '159' union
select '160' union
select '162' union
select '165' union
select '166' union
select '171' union
select '187' union
select '203' union
select '208' union
select '215' union
select '221' union
select '222' union
select '223' union
select '224' union
select '228' union
select '255'
) tx
join lateral (select * from fbvenval(tx.co_bloque)) vb on true
order by 1,2 