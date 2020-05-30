
BEGIN TRAN;

SET NOCOUNT ON;

CREATE TABLE [Security].[__TestProc__CheckRights] (
    [user_id] BIGINT
    ,[project_id] BIGINT
    ,[table_name] NVARCHAR(255)
    ,[operation] NVARCHAR(64)
)
GO

ALTER PROCEDURE [Security].[CheckRights]
    @user_id BIGINT
    ,@project_id BIGINT
    ,@table_name NVARCHAR(255)
    ,@operation NVARCHAR(64)
AS
    INSERT INTO [Security].[__TestProc__CheckRights] ([user_id], [project_id], [table_name], [operation])
    VALUES (@user_id, @project_id, @table_name, @operation)

RETURN
GO

    

DECLARE @user_merge_result TABLE ([op] NVARCHAR(64), [user_id] BIGINT);
INSERT INTO @user_merge_result
EXEC [Entity].[UserMerge] 0, 'name - dummy value', 'email - dummy value', 'ext_id - dummy value', 'oauth_service_name - dummy value';

DECLARE @user_id BIGINT = (SELECT TOP 1 [user_id] FROM @user_merge_result);
DECLARE @user_tmp TABLE ([user_id] BIGINT, [user_name] NVARCHAR(255), [user_email] NVARCHAR(255), [user_ext_id] NVARCHAR(255), [user_oauth_service_name] NVARCHAR(255));
INSERT INTO @user_tmp
EXEC [Entity].[UserList] ;

INSERT INTO @user_tmp([user_id], [user_name], [user_email], [user_ext_id], [user_oauth_service_name])
VALUES (@user_id, 'name - dummy value', 'email - dummy value', 'ext_id - dummy value', 'oauth_service_name - dummy value');

DECLARE @user_res_cnt INT = (SELECT
    COUNT(*)
FROM
    (SELECT DISTINCT * FROM @user_tmp WHERE [user_id] = @user_id) AS t);
IF @user_res_cnt > 1
BEGIN
    THROW 51000, 'FAIL: List or Insert', 1;
END;

DELETE FROM @user_tmp;

EXEC [Entity].[UserDelete] @user_id;

INSERT INTO @user_tmp
EXEC [Entity].[UserList] ;

SET @user_res_cnt = (SELECT
    COUNT(*)
FROM
    (SELECT DISTINCT * FROM @user_tmp WHERE [user_id] = @user_id) AS t);
IF @user_res_cnt > 0
BEGIN
    THROW 51000, 'FAIL: List or Delete', 1;
END;

    

DECLARE @category_merge_result TABLE ([op] NVARCHAR(64), [category_id] BIGINT);
INSERT INTO @category_merge_result
EXEC [Entity].[CategoryMerge] 0, 123, 321, 0, 0, 'name - dummy value', 1, 2, 'img_url - dummy value', 1;

DECLARE @category_id BIGINT = (SELECT TOP 1 [category_id] FROM @category_merge_result);
DECLARE @category_tmp TABLE ([category_id] BIGINT, [category_parent_id] BIGINT, [category_is_folder] BIT, [category_name] NVARCHAR(255), [category_is_minus] BIT, [category_sort] REAL, [category_img_url] NVARCHAR(MAX), [category_visible] BIT);
INSERT INTO @category_tmp
EXEC [Entity].[CategoryList] 123,321,0;

INSERT INTO @category_tmp([category_id], [category_parent_id], [category_is_folder], [category_name], [category_is_minus], [category_sort], [category_img_url], [category_visible])
VALUES (@category_id, 0, 0, 'name - dummy value', 1, 2, 'img_url - dummy value', 1);

DECLARE @category_res_cnt INT = (SELECT
    COUNT(*)
FROM
    (SELECT DISTINCT * FROM @category_tmp WHERE [category_id] = @category_id) AS t);
IF @category_res_cnt > 1
BEGIN
    THROW 51000, 'FAIL: List or Insert', 1;
END;

DELETE FROM @category_tmp;

EXEC [Entity].[CategoryDelete] @category_id,123,321;

INSERT INTO @category_tmp
EXEC [Entity].[CategoryList] 123,321,0;

SET @category_res_cnt = (SELECT
    COUNT(*)
FROM
    (SELECT DISTINCT * FROM @category_tmp WHERE [category_id] = @category_id) AS t);
IF @category_res_cnt > 0
BEGIN
    THROW 51000, 'FAIL: List or Delete', 1;
END;

    

SELECT * FROM [Security].[__TestProc__CheckRights]

ROLLBACK TRAN
    
