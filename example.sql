
IF SCHEMA_ID('Entity') IS NULL
    EXEC('CREATE SCHEMA Entity');
GO

IF SCHEMA_ID('Security') IS NULL
    EXEC('CREATE SCHEMA Security');
GO

IF object_id('[Security].[CheckRights]', 'P') IS NOT NULL
    DROP PROCEDURE [Security].[CheckRights]
GO

CREATE PROCEDURE  [Security].[CheckRights]
    @user_id BIGINT
    ,@project_id BIGINT
    ,@operation NVARCHAR(64)
AS
    -- CHECK RIGHTS IS HERE --
GO
    
IF object_id('[Entity].[Users]', 'U') IS NOT NULL
    DROP TABLE [Entity].[Users]
GO

CREATE TABLE [Entity].[Users] (
    [user_id] BIGINT IDENTITY(1,1) NOT NULL
    ,[user_name] NVARCHAR(255) NULL
    ,[user_email] NVARCHAR(255) NULL
    ,[user_ext_id] NVARCHAR(255) NULL
    ,[user_oauth_service_name] NVARCHAR(255) NULL
    ,[is_delete] BIT NOT NULL
)
GO

IF object_id('[Entity].[UserList]', 'P') IS NOT NULL
    DROP PROCEDURE [Entity].[UserList]
GO

CREATE PROCEDURE  [Entity].[UserList]
AS

    SET NOCOUNT ON;

    SELECT
        t.[user_id] [user_id]
        ,t.[user_name] [user_name]
        ,t.[user_email] [user_email]
        ,t.[user_ext_id] [user_ext_id]
        ,t.[user_oauth_service_name] [user_oauth_service_name]
    FROM
        [Entity].[Users] AS t
    WHERE
        t.is_delete = 0
    ;
RETURN
GO

IF object_id('[Entity].[UserMerge]', 'P') IS NOT NULL
    DROP PROCEDURE [Entity].[UserMerge]
GO

CREATE PROCEDURE  [Entity].[UserMerge]
    @user_id BIGINT
    ,@user_name NVARCHAR(255)
    ,@user_email NVARCHAR(255)
    ,@user_ext_id NVARCHAR(255)
    ,@user_oauth_service_name NVARCHAR(255)
AS

    DECLARE @processed_record_table TABLE (id BIGINT);

    SET NOCOUNT ON;

    MERGE [Entity].[Users] AS dst
    USING (
        SELECT
            @user_id AS [user_id]
            ,0 AS [is_delete]
            ,@user_name AS [user_name]
            ,@user_email AS [user_email]
            ,@user_ext_id AS [user_ext_id]
            ,@user_oauth_service_name AS [user_oauth_service_name]
    ) AS src
    ON (dst.user_id = src.user_id)
    WHEN MATCHED THEN
        UPDATE SET
            [is_delete] = src.[is_delete]
            ,[user_name] = src.[user_name]
            ,[user_email] = src.[user_email]
            ,[user_ext_id] = src.[user_ext_id]
            ,[user_oauth_service_name] = src.[user_oauth_service_name]
    WHEN NOT MATCHED THEN
        INSERT (
            [is_delete]
            ,[user_name]
            ,[user_email]
            ,[user_ext_id]
            ,[user_oauth_service_name]
        ) VALUES (
            src.[is_delete]
            ,src.[user_name]
            ,src.[user_email]
            ,src.[user_ext_id]
            ,src.[user_oauth_service_name]
    ) OUTPUT
        $ACTION AS [action], ISNULL(DELETED.[user_id], INSERTED.[user_id]) AS user_id;
RETURN
GO

IF object_id('[Entity].[UserDelete]', 'P') IS NOT NULL
    DROP PROCEDURE [Entity].[UserDelete]
GO

CREATE PROCEDURE  [Entity].[UserDelete]
    @user_id BIGINT
AS

    SET NOCOUNT ON;

    UPDATE [Entity].[Users] SET
        is_delete = 1
    WHERE
        user_id = @user_id

RETURN
GO

IF object_id('[Entity].[Categories]', 'U') IS NOT NULL
    DROP TABLE [Entity].[Categories]
GO

CREATE TABLE [Entity].[Categories] (
    [category_id] BIGINT IDENTITY(1,1) NOT NULL
    ,[category_parent_id] BIGINT NOT NULL
    ,[category_is_folder] BIT NOT NULL
    ,[project_id] BIGINT NOT NULL
    ,[category_name] NVARCHAR(255) NULL
    ,[category_is_minus] BIT NULL
    ,[category_sort] REAL NULL
    ,[category_img_url] NVARCHAR(MAX) NULL
    ,[category_visible] BIT NULL
    ,[is_delete] BIT NOT NULL
)
GO

IF object_id('[Entity].[CategoryList]', 'P') IS NOT NULL
    DROP PROCEDURE [Entity].[CategoryList]
GO

CREATE PROCEDURE  [Entity].[CategoryList]
    @user_id BIGINT
    ,@project_id BIGINT
    ,@category_parent_id BIGINT
AS

    EXEC [Security].[CheckRights] @user_id, @project_id, 'List';
    SET NOCOUNT ON;

    SELECT
        t.[category_id] [category_id]
        ,t.[category_parent_id] [category_parent_id]
        ,t.[category_is_folder] [category_is_folder]
        ,t.[category_name] [category_name]
        ,t.[category_is_minus] [category_is_minus]
        ,t.[category_sort] [category_sort]
        ,t.[category_img_url] [category_img_url]
        ,t.[category_visible] [category_visible]
    FROM
        [Entity].[Categories] AS t
    WHERE
        t.is_delete = 0
        AND t.project_id = project_id
        AND t.category_parent_id = category_parent_id
    ;
RETURN
GO

IF object_id('[Entity].[CategoryMerge]', 'P') IS NOT NULL
    DROP PROCEDURE [Entity].[CategoryMerge]
GO

CREATE PROCEDURE  [Entity].[CategoryMerge]
    @category_id BIGINT
    ,@user_id BIGINT
    ,@project_id BIGINT
    ,@category_parent_id BIGINT
    ,@category_is_folder BIT
    ,@category_name NVARCHAR(255)
    ,@category_is_minus BIT
    ,@category_sort REAL
    ,@category_img_url NVARCHAR(MAX)
    ,@category_visible BIT
AS

    DECLARE @processed_record_table TABLE (id BIGINT);

    SET NOCOUNT ON;

    EXEC [Security].[CheckRights] @user_id, @project_id, 'Merge';

    MERGE [Entity].[Categories] AS dst
    USING (
        SELECT
            @category_id AS [category_id]
            ,0 AS [is_delete]
            ,@project_id AS [project_id]
            ,@category_parent_id AS [category_parent_id]
            ,@category_is_folder AS [category_is_folder]
            ,@user_id AS [last_hand_user_id]
            ,@category_name AS [category_name]
            ,@category_is_minus AS [category_is_minus]
            ,@category_sort AS [category_sort]
            ,@category_img_url AS [category_img_url]
            ,@category_visible AS [category_visible]
    ) AS src
    ON (dst.category_id = src.category_id)
    WHEN MATCHED THEN
        UPDATE SET
            [is_delete] = src.[is_delete]
            ,[project_id] = src.[project_id]
            ,[category_parent_id] = src.[category_parent_id]
            ,[category_is_folder] = src.[category_is_folder]
            ,[last_hand_user_id] = src.[last_hand_user_id]
            ,[category_name] = src.[category_name]
            ,[category_is_minus] = src.[category_is_minus]
            ,[category_sort] = src.[category_sort]
            ,[category_img_url] = src.[category_img_url]
            ,[category_visible] = src.[category_visible]
    WHEN NOT MATCHED THEN
        INSERT (
            [is_delete]
            ,[project_id]
            ,[category_parent_id]
            ,[category_is_folder]
            ,[last_hand_user_id]
            ,[category_name]
            ,[category_is_minus]
            ,[category_sort]
            ,[category_img_url]
            ,[category_visible]
        ) VALUES (
            src.[is_delete]
            ,src.[project_id]
            ,src.[category_parent_id]
            ,src.[category_is_folder]
            ,src.[last_hand_user_id]
            ,src.[category_name]
            ,src.[category_is_minus]
            ,src.[category_sort]
            ,src.[category_img_url]
            ,src.[category_visible]
    ) OUTPUT
        $ACTION AS [action], ISNULL(DELETED.[category_id], INSERTED.[category_id]) AS category_id;
RETURN
GO

IF object_id('[Entity].[CategoryDelete]', 'P') IS NOT NULL
    DROP PROCEDURE [Entity].[CategoryDelete]
GO

CREATE PROCEDURE  [Entity].[CategoryDelete]
    @category_id BIGINT
    ,@user_id BIGINT
    ,@project_id BIGINT
AS

    SET NOCOUNT ON;

    EXEC [Security].[CheckRights] @user_id, @project_id, 'Delete';

    UPDATE [Entity].[Categories] SET
        is_delete = 1
    WHERE
        category_id = @category_id

RETURN
GO

