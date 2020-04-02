IF object_id('[Entity].[Users]', 'U') IS NOT NULL
    DROP TABLE [Entity].[Users]
GO

CREATE TABLE [Entity].[Users] (
    [user_id] BIGINT IDENTITY(1,1) NOT NULL,
    [user_name] NVARCHAR(255) NOT NULL
    [user_email] NVARCHAR(255) NOT NULL
    [user_ext_id] NVARCHAR(255) NULL
    [user_oauth_service_name] NVARCHAR(255) NOT NULL
    [is_delete] BIT NOT NULL
)
GO

IF object_id('[Entity].[UserList]', 'P') IS NOT NULL
    DROP PROCEDURE [Entity].[UserList]
GO

CREATE PROCEDURE  [Entity].[UserList]
AS

    -- здесь будет проверка прав --

    SET NOCOUNT ON;

    SELECT
        t.[user_id] [user_id]
        t.[user_name] [user_name]
        t.[user_email] [user_email]
        t.[user_ext_id] [user_ext_id]
        t.[user_oauth_service_name] [user_oauth_service_name]
    FROM
        [Entity].[Users] AS t
    WHERE
        t.is_delete = 0
    ;
RETURN
GO

IF object_id('[Entity].[Categories]', 'U') IS NOT NULL
    DROP TABLE [Entity].[Categories]
GO

CREATE TABLE [Entity].[Categories] (
    [category_id] BIGINT IDENTITY(1,1) NOT NULL,
    [category_parent_id] BIGINT NOT NULL,
    [category_is_folder] BIT NOT NULL,
    [project_id] BIGINT NOT NULL,
    [category_name] NVARCHAR(255) NOT NULL
    [category_is_minus] BIT NOT NULL
    [category_sort] REAL NOT NULL
    [category_img_url] NVARCHAR(MAX) NOT NULL
    [category_visible] BIT NOT NULL
    [last_hand_user_id] BIGINT NOT NULL,
    [is_delete] BIT NOT NULL
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

    -- здесь будет проверка прав --

    SET NOCOUNT ON;

    SELECT
        t.[category_id] [category_id]
        t.[category_parent_id] [category_parent_id]
        t.[category_is_folder] [category_is_folder]
        t.[category_name] [category_name]
        t.[category_is_minus] [category_is_minus]
        t.[category_sort] [category_sort]
        t.[category_img_url] [category_img_url]
        t.[category_visible] [category_visible]
    FROM
        [Entity].[Categories] AS t
    WHERE
        t.is_delete = 0
        AND t.project_id = project_id
        AND t.category_parent_id = category_parent_id
    ;
RETURN
GO

