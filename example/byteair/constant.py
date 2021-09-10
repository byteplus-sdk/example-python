"""
租户相关信息
"""
# 字节侧提供，用于签名
TOKEN = "xxxxxxxxxxxx"

# 火山引擎申请的账号id/租户id(tenant_id)，如"2100021"
TENANT_ID = "xxxxxxxxxxxx"

# 个性化推荐服务新建的项目id(project_id)，如"1231314"
PROJECT_ID = "xxxxxxxxxxxx"

"""
stage枚举值，与推荐平台四种同步阶段相对应
"""
STAGE_INCREMENTAL_SYNC_STREAMING = "incremental_sync_streaming"  # 增量实时数据同步阶段

STAGE_INCREMENTAL_SYNC_DAILY = "incremental_sync_daily"  # 增量天级数据同步阶段

STAGE_PRE_SYNC = "pre_sync"  # 测试数据/预同步阶段

STAGE_HISTORY_SYNC = "history_sync"  # 历史数据同步阶段

"""
标准数据topic枚举值，包括：item(物品，如商品、媒资数据、社区内容等)、user(用户)、behavior(行为)
"""
TOPIC_ITEM = "item"  # 物品

TOPIC_USER = "user"  # 用户

TOPIC_BEHAVIOR = "behavior"  # 行为
