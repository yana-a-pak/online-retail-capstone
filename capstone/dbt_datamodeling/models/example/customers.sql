SELECT 
    CustomerID,
    InvoiceNo AS Invoice_No,
    Description AS Item_Name,
    UnitPrice AS Price,
    Quantity AS QTY,
    Quantity * UnitPrice AS Amount,
    FORMAT_DATE('%Y-%m-%d',InvoiceDate) AS Invoice_Date,
    Country
FROM 
    capstonedw1.raw_invoicesa.retails
WHERE 
    CustomerID IS NOT NULL 
    AND UnitPrice IS NOT NULL 
    AND UnitPrice > 0