

--ADD NEW ROWS
INSERT INTO [JiLi].[dbo].[MA_2A_Form_Manual_Mapping] (
    rmv_name,
    mass_gov_name,
    address,
    city,
    state,
    zip,
    phone,
    add_dt
)
VALUES (
    'Test RMV Name (Manual)',       -- rmv_name
    'Test Mass Gov Equivalent',     -- mass_gov_name
    '123 Main St',                  -- address
    'Quincy',                       -- city
    'MA',                           -- state
    '02169',                        -- zip
    '413-111-0000',                 -- phone
    GETDATE()                       -- add_dt
);
----UPDATE EXISTING ROWS
UPDATE [JiLi].[dbo].[MA_2A_Form_Manual_Mapping]
SET phone = '4132227999'
WHERE rmv_name ='Test RMV Name (Manual)';

--DELETE ROWS
DELETE FROM [JiLi].[dbo].[MA_2A_Form_Manual_Mapping]
WHERE [rmv_name] = 'Test RMV Name (Manual)';


--CHECK RESULT
select * from [JiLi].[dbo].[MA_2A_Form_Manual_Mapping] 



