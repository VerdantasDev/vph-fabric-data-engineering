-- CREATE TABLE Engineering.EmployeeData (
--     EmpId       INT          NOT NULL,
--     EmpName     VARCHAR(100) NOT NULL,
--     Department  VARCHAR(100) NOT NULL,
--     Salary      INT          NOT NULL    -- sensitive column (CLS target)
-- );
-- GO
-- INSERT INTO Engineering.EmployeeData (EmpId, EmpName, Department, Salary) VALUES
-- (1, 'Harish Kamunuri', 'Data Engineering', 90000),
-- (2, 'Ashmit Singh',    'Data Engineering', 85000),
-- (3, 'Maitri Dixit',    'Analytics',        95000),
-- (4, 'Uday Mella',      'Leadership',       120000);   -- row hidden from others (RLS target)
-- GO
-- Deny the Salary column to every other engineer (add/remove emails as needed)
-- DENY SELECT ON Engineering.EmployeeData(Salary) TO [amsingh@verdantas.com];
-- DENY SELECT ON Engineering.EmployeeData(Salary) TO [mdixit@verdantas.com];
-- DENY SELECT ON Engineering.EmployeeData(Salary) TO [szagade@verdantas.com];
-- GO
CREATE FUNCTION Engineering.fn_rls_employee(@EmpId INT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS fn_result
    WHERE USER_NAME() = 'hkamunuri@verdantas.com'   -- full access
       OR @EmpId <> 4;                               -- others: hide EmpId = 4