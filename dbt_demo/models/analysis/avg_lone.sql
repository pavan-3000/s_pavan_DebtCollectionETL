SELECT 
    AVG(Loan_Amount) AS average_loan_amount
FROM 
    {{ source('db','borrow') }}
WHERE 
    DAYS_LEFT_TO_PAY_CURRENT_EMI > 5
