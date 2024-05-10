SELECT DISTINCT
    InvoiceNo,
    StockCode AS product_id,
    Description AS Item,
    UnitPrice,
    Quantity,
    Quantity * UnitPrice AS total,
    FORMAT_DATE('%Y-%m-%d',InvoiceDate) AS Invoice_Date,
    CustomerID
FROM 
    capstonedw1.raw_invoicesa.retails

WHERE UnitPrice IS NOT NULL 
    AND CustomerID IS NOT NULL 
    AND UnitPrice > 0