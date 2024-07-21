SELECT Loan_Type,
       COUNT(*) AS Number_of_Borrowers,
       AVG(Loan_Amount) AS Average_Loan_Amount,
       AVG(Interest_Rate) AS Average_Interest_Rate,
       SUM(EMI) AS Total_EMI
FROM  {{ source('db','borrow') }}
GROUP BY Loan_Type