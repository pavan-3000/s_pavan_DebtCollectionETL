SELECT Name
FROM 
{{ source('db','borrow') }}
WHERE
 Delayed_Payment = 'No'