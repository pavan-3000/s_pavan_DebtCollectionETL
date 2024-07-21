SELECT 
    Name,
    LOAN_AMOUNT  AS outstanding_balance
FROM 
    {{ source('db','borrow') }}
ORDER BY 
    outstanding_balance DESC
LIMIT 10
