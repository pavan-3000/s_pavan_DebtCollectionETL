SELECT 
    Name,
    Loan_Amount,
    EMI,
    Repayment_History
FROM 
    {{ source('db','borrow') }}
WHERE 
    Repayment_History = 'Good'
