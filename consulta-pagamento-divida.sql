with divida as (
    SELECT row_number() over (partition by lf.SEQ_DIVIDA ORDER BY lf.DTA_LANCAMENTO) as num_divida,  
        lf.SEQ_DIVIDA, 
        lf.TXT_OBSERVACAO, 
        lf.DTA_LANCAMENTO, lf.VAL_PAGAMENTO,
        SUM(lf.VAL_PAGAMENTO) OVER (PARTITION BY lf.SEQ_DIVIDA ORDER BY lf.DTA_LANCAMENTO) AS VALOR_ACUMULADO_DIVIDA
    FROM <tabela_divida> lf
),
pagamento as (
    SELECT row_number() over (partition by dhr.seq_recebedor_rubrica order by dhr.ano_pag, dhr.mes_pag) as num_pagamento,  
        dhr.SEQ_RECEBEDOR_RUBRICA, 
        dhr.MES_PAG, 
        dhr.ANO_PAG, 
        dsr.VAL_RUBRICA,
        SUM(dsr.VAL_RUBRICA) OVER (PARTITION BY dhr.SEQ_RECEBEDOR_RUBRICA ORDER BY dhr.ANO_PAG, dhr.MES_PAG) AS VALOR_PAGO_ACUMULADO
    FROM <tabela_pagamento> dhr, <folha_pagamento> dsr
    WHERE dhr.ANO_PAG = dsr.ANO_PAG
        AND dhr.MES_PAG = dsr.MES_PAG
        AND dhr.COD_RUBRICA = dsr.COD_RUBRICA
        AND dhr.COD_SERVIDOR = dsr.COD_SERVIDOR
        AND dhr.NUM_SEQ_RUBRICA = dsr.NUM_SEQ_RUBRICA
),
divida_pagamento (
        --NIVEL, 
        SEQ_DIVIDA, 
        TXT_OBSERVACAO, 
        NUM_DIVIDA,
        DTA_LANCAMENTO, 
        VAL_PAGAMENTO,
        VALOR_ACUMULADO_DIVIDA,
        NUM_PAGAMENTO, 
        ANO_PAG,
        MES_PAG, 
        VAL_RUBRICA, 
        VALOR_PAGO_ACUMULADO,
        VALOR_PAGO, 
        SALDO_DIVIDA,
        SALDO_RUBRICA
    ) as (
        
    select --1 as nivel,
        d.SEQ_DIVIDA, 
        d.TXT_OBSERVACAO, 
        d.NUM_DIVIDA,
        d.DTA_LANCAMENTO, 
        d.VAL_PAGAMENTO,
        d.VALOR_ACUMULADO_DIVIDA, 
        p.num_pagamento,
        p.ano_pag,
        p.mes_pag, 
        p.val_rubrica, 
        p.valor_pago_acumulado,
        case 
            when d.val_pagamento > p.val_rubrica then p.val_rubrica 
            when p.num_pagamento is not null then d.val_pagamento 
        end as valor_pago, 
        case 
            when d.val_pagamento > p.val_rubrica then d.val_pagamento - p.val_rubrica 
            when p.num_pagamento is not null then 0
        end as saldo_divida,
        case 
            when d.val_pagamento >= p.val_rubrica then 0 
            else p.val_rubrica - d.val_pagamento 
        end as saldo_rubrica
    from divida d
        left join pagamento p on p.seq_recebedor_rubrica = d.seq_divida and p.num_pagamento = 1 
    where d.num_divida = 1
    
    union all
    
    select --dp.nivel + 1, 
        d.SEQ_DIVIDA, 
        d.TXT_OBSERVACAO, 
        d.NUM_DIVIDA,
        d.DTA_LANCAMENTO, 
        d.VAL_PAGAMENTO,
        d.VALOR_ACUMULADO_DIVIDA, 
        p.num_pagamento,
        p.ano_pag,
        p.mes_pag, 
        p.val_rubrica, 
        p.valor_pago_acumulado,
        case 
            when dp.num_divida = d.num_divida and dp.saldo_divida >= p.val_rubrica then p.val_rubrica 
            when dp.num_divida = d.num_divida and dp.saldo_divida < p.val_rubrica then dp.saldo_divida
            -- Mudança de dívida sem mudança de pagamento 
            when dp.num_divida + 1 = d.num_divida and dp.num_pagamento = p.num_pagamento and d.val_pagamento >= dp.saldo_rubrica then dp.saldo_rubrica
            when dp.num_divida + 1 = d.num_divida and dp.num_pagamento = p.num_pagamento and d.val_pagamento < dp.saldo_rubrica then d.val_pagamento
            -- Mudança de dívida e pagamento
            when dp.num_divida + 1 = d.num_divida and dp.num_pagamento + 1 = p.num_pagamento and d.val_pagamento >= p.val_rubrica then p.val_rubrica
            when dp.num_divida + 1 = d.num_divida and dp.num_pagamento + 1 = p.num_pagamento and d.val_pagamento < p.val_rubrica then d.val_pagamento
            
            else null 
        end as valor_pago, 
        case 
            when dp.num_divida = d.num_divida and dp.saldo_divida > p.val_rubrica then dp.saldo_divida - p.val_rubrica 
            -- Mudança de dívida sem mudança de pagamento
            when dp.num_divida + 1 = d.num_divida and dp.num_pagamento = p.num_pagamento and d.val_pagamento > dp.saldo_rubrica then d.val_pagamento - dp.saldo_rubrica 
            -- Mudança de dívida e pagamento
            when dp.num_divida + 1 = d.num_divida and dp.num_pagamento + 1 = p.num_pagamento and d.val_pagamento > p.val_rubrica then d.val_pagamento - p.val_rubrica 

            when p.num_pagamento is not null then 0 
        end as saldo_divida,
        case 
            when dp.num_divida = d.num_divida and dp.saldo_divida >= p.val_rubrica then 0 
            when dp.num_divida = d.num_divida and dp.saldo_divida < p.val_rubrica then p.val_rubrica - dp.saldo_divida
            -- Mudança de dívida sem mudança de pagamento 
            when dp.num_divida + 1 = d.num_divida and dp.num_pagamento = p.num_pagamento and d.val_pagamento >= dp.saldo_rubrica then 0
            when dp.num_divida + 1 = d.num_divida and dp.num_pagamento = p.num_pagamento and d.val_pagamento < dp.saldo_rubrica then dp.saldo_rubrica - d.val_pagamento
            -- Mudança de dívida e pagamento
            when dp.num_divida + 1 = d.num_divida and dp.num_pagamento + 1 = p.num_pagamento and d.val_pagamento >= p.val_rubrica then 0
            when dp.num_divida + 1 = d.num_divida and dp.num_pagamento + 1 = p.num_pagamento and d.val_pagamento < p.val_rubrica then p.val_rubrica - d.val_pagamento
            
            else null 
        end as saldo_rubrica
    from divida_pagamento dp
        inner join divida d on d.seq_divida = dp.seq_divida and d.num_divida = dp.num_divida + case when dp.saldo_divida > 0 then 0 else 1 end
        left join pagamento p on p.seq_recebedor_rubrica = dp.seq_divida and p.num_pagamento = dp.num_pagamento + case when dp.saldo_rubrica > 0 then 0 else 1 end 

)
select * 
from divida_pagamento 
where seq_divida = 8332 -- 1415 -- 8332
;
