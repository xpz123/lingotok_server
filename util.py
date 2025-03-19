import json

#,,,邮箱,DIY,艺术,动漫漫画,生活小窍门,户外,解压/助眠/令人满足的（ASMR/刮肥皂/太空沙/液压机等等）,,美妆/穿搭,科学教育,家庭,园艺
def online_interest_mapping(online_interest):
    mapping = {
        "technology": ["汽车", "科学教育"],
        "sport": ["运动", "户外", "健身/健康"],
        "music": ["音乐", "小品/脱口秀/相声", "舞蹈"],
        "travel": ["旅行", "vlog", "户外", "食物/饮料", "健身/健康", "宠物/动物"],
        "finance": ["人生建议"],
        "car": ["汽车"]
    }
    return mapping.get(online_interest.lower(), [])