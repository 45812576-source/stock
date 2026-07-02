"""产业链静态配置 — config/chain_config.py
股票名称需与 stock_info.stock_name 一致（用于代码解析）
"""

CHAIN_ORDER = [
    "新能源车",
    "半导体",
    "AI算力基础设施",
    "光伏",
    "风电",
    "储能",
    "电力设备",
    "军工",
    "消费电子",
    "智能驾驶",
    "能源电力运营",
    "化工新材料",
    "钢铁基建材料",
    "碳中和环保",
    "医药CRO",
    "稀土永磁",
    "氢能",  # auto-added
    "工程机械",  # auto-added
    "商用车",  # auto-added
    "消费品牌",  # auto-added
    "金融服务",  # auto-added
    "医疗器械",  # auto-added
    "战略金属",  # auto-added
    "在线旅游",  # auto-added
    "油气设备",  # auto-added
    "跨境电商",  # auto-added
    "金刚石散热",  # auto-added
]

CHAINS = {
    "新能源车": {
        "icon": "electric_car",
        "color": "#10b981",
        "tiers": {
            "上上游": {
                "label": "资源（锂/钴/镍）",
                "stocks": ["赣锋锂业", "天齐锂业", "华友钴业", "洛阳钼业", "盛新锂能", "盐湖股份",  # auto-added
                    "国城矿业",  # auto-added
                    "大中矿业",  # auto-added
                    "仙鹤股份",  # auto-added
                    "时代新材",  # auto-added
                    "兴发集团",  # auto-added
                    "中钨高新",  # auto-added
                    "滨化股份",  # auto-added
                    "天岳先进",  # auto-added
                    "三安光电",  # auto-added
                    "兴福电子",  # auto-added
                    "云南锗业",  # auto-added
                    "沃格光电",  # auto-added
                    "凯盛科技",  # auto-added
                    "鼎龙股份",  # auto-added
                    "飞凯材料",  # auto-added
                    "彤程新材",  # auto-added
                    "国瓷材料",  # auto-added
                    "博迁新材",  # auto-added
                    "洁美科技",  # auto-added
                    "神工股份",  # auto-added
                    "莲花控股",  # auto-added
                    "中矿资源",  # auto-added
                    "力诺药包",  # auto-added
                    "容大感光",  # auto-added
                    "八亿时空",  # auto-added
                    "圣泉集团",  # auto-added
                    "东材科技",  # auto-added
                    "久日新材",  # auto-added
                    "强力新材",  # auto-added
                    "万润股份",  # auto-added
                    "瑞联新材",  # auto-added
                    "天承科技",  # auto-added
                    "宸展光电",  # auto-added
                    "天华新能"],
            },
            "上游": {
                "label": "材料（正极/负极/隔膜/电解液/铜箔）",
                "stocks": [
                    "容百科技", "当升科技", "德方纳米",  # 正极/前驱体（中伟股份待补录）
                    "璞泰来", "杉杉股份", "中科电气",               # 负极
                    "恩捷股份", "星源材质",                          # 隔膜
                    "天赐材料", "新宙邦",                            # 电解液
                    "诺德股份", "嘉元科技",                          # 铜箔,  # auto-added
                    "帝尔激光",  # auto-added
                    "大族激光",  # auto-added
                    "东威科技",  # auto-added
                    "星宸科技",  # auto-added
                    "拓荆科技",  # auto-added
                    "华海清科",  # auto-added
                    "盛美上海",  # auto-added
                    "鸿仕达",  # auto-added
                    "晶升股份",  # auto-added
                    "新益昌",  # auto-added
                    "联动科技",  # auto-added
                    "精智达",  # auto-added
                    "金海通",  # auto-added
                    "强一股份",  # auto-added
                    "亚威股份",  # auto-added
                    "芯碁微装",  # auto-added
                    "华盛锂电",  # auto-added
                    "海科新源",  # auto-added
                    "富祥药业",  # auto-added
                    "孚日股份",  # auto-added
                    "海辰药业",  # auto-added
                    "ASML",  # auto-added
                    "英杰电气",  # auto-added
                    "恒运昌",  # auto-added
                    "沐曦股份",  # auto-added
                    "天数智芯",  # auto-added
                    "南亚新材",  # auto-added
                    "智立方",  # auto-added
                    "盛剑科技",  # auto-added
                    "高测股份",  # auto-added
                    "建滔积层板",  # auto-added
                    "德福科技",  # auto-added
                    "铜冠铜箔",  # auto-added
                    "海亮股份",  # auto-added
                    "洪田股份",  # auto-added
                    "泰金新能",  # auto-added
                    "艾华集团",  # auto-added
                    "江海股份",  # auto-added
                    "万泽股份",  # auto-added
                    "海光信息",  # auto-added
                    "云南锗业",  # auto-added
                    "横店东磁",  # auto-added
                    "臻宝科技",  # auto-added
                    "海星股份",  # auto-added
                    "王子新材",  # auto-added
                    "亚翔集成",  # auto-added
                    "万裕科技",  # auto-added
                    "立隆电",  # auto-added
                    "中国稀土",  # auto-added
                    "宏明电子",  # auto-added
                    "盛科通信",  # auto-added
                    "西部材料",  # auto-added
                    "中航高科",  # auto-added
                    "信维通信",  # auto-added
                    "斯瑞新材",  # auto-added
                    "雅化集团",  # auto-added
                    "建滔集团",  # auto-added
                    "华正新材",  # auto-added
                    "金安国际",  # auto-added
                    "国际复材",  # auto-added
                    "元力股份",  # auto-added
                    "汇成真空",  # auto-added
                    "中导光电",  # auto-added
                    "半导体设备板块",  # auto-added
                    "微导纳米",  # auto-added
                    "宝明科技",  # auto-added
                    "方源新能源",  # auto-added
                    "台新股份",  # auto-added
                    "正帆科技",  # auto-added
                    "先锋精科",  # auto-added
                    "富创精密",  # auto-added
                    "新莱应材",  # auto-added
                    "隆达股份",  # auto-added
                    "石大胜华",  # auto-added
                    "永和股份",  # auto-added
                    "楚江新材",  # auto-added
                    "英伟达",  # auto-added
                    "AMD",  # auto-added
                    "汉钟精机",  # auto-added
                    "中科仪",  # auto-added
                    "联讯仪器",  # auto-added
                    "精测电子",  # auto-added
                    "东岳集团"],
            },
            "中游": {
                "label": "动力电池 & 电机电控",
                "stocks": ["宁德时代", "亿纬锂能", "国轩高科", "汇川技术", "卧龙电驱", "大洋电机",  # auto-added
                    "朝阳科技",  # auto-added
                    "中际旭创",  # auto-added
                    "新易盛",  # auto-added
                    "工业富联",  # auto-added
                    "金富科技",  # auto-added
                    "五洋自控",  # auto-added
                    "英维克",  # auto-added
                    "思泉新材",  # auto-added
                    "强瑞技术",  # auto-added
                    "东阳光",  # auto-added
                    "秦淮数据",  # auto-added
                    "潍柴动力",  # auto-added
                    "杰瑞股份",  # auto-added
                    "中国动力",  # auto-added
                    "豪迈科技",  # auto-added
                    "汽轮科技",  # auto-added
                    "潍柴重机",  # auto-added
                    "联德股份",  # auto-added
                    "金盘科技",  # auto-added
                    "四方股份",  # auto-added
                    "长飞光纤",  # auto-added
                    "亨通光电",  # auto-added
                    "中天科技",  # auto-added
                    "永鼎股份",  # auto-added
                    "舜宇光学科技",  # auto-added
                    "茂莱光学",  # auto-added
                    "康宁",  # auto-added
                    "川润股份",  # auto-added
                    "同星科技",  # auto-added
                    "胜蓝股份",  # auto-added
                    "美利信",  # auto-added
                    "中鼎股份",  # auto-added
                    "溯联股份",  # auto-added
                    "川环科技",  # auto-added
                    "鸿富瀚",  # auto-added
                    "锦富技术",  # auto-added
                    "大元泵业",  # auto-added
                    "飞龙股份",  # auto-added
                    "国瓷材料",  # auto-added
                    "宏达电子",  # auto-added
                    "太辰光",  # auto-added
                    "仕佳光子",  # auto-added
                    "衡东光",  # auto-added
                    "航天电子",  # auto-added
                    "国博电子",  # auto-added
                    "陕西华达",  # auto-added
                    "智明达",  # auto-added
                    "六九一二",  # auto-added
                    "上海瀚讯",  # auto-added
                    "高华科技",  # auto-added
                    "西测测试",  # auto-added
                    "铂力特",  # auto-added
                    "通鼎互联",  # auto-added
                    "杭电股份",  # auto-added
                    "特发信息",  # auto-added
                    "宏柏新材",  # auto-added
                    "东方国信",  # auto-added
                    "大位科技",  # auto-added
                    "奥飞数据",  # auto-added
                    "万国数据",  # auto-added
                    "世纪互联",  # auto-added
                    "华盛昌"],
            },
            "下游": {
                "label": "整车 & 零部件",
                "stocks": ["比亚迪", "长安汽车", "长城汽车", "北汽蓝谷", "拓普集团", "均胜电子", "宁波华翔",  # auto-added
                    "胜宏科技",  # auto-added
                    "世运电路",  # auto-added
                    "奥士康",  # auto-added
                    "极氪",  # auto-added
                    "吉利汽车",  # auto-added
                    "小鹏汽车",  # auto-added
                    "东方日升",  # auto-added
                    "东威科技",  # auto-added
                    "鼎泰高科",  # auto-added
                    "京东方A",  # auto-added
                    "银轮股份",  # auto-added
                    "天润工业",  # auto-added
                    "中富电路",  # auto-added
                    "洁美科技",  # auto-added
                    "博迁新材",  # auto-added
                    "国瓷材料",  # auto-added
                    "三环集团",  # auto-added
                    "风华高科",  # auto-added
                    "艾华集团",  # auto-added
                    "金富科技",  # auto-added
                    "科翔股份",  # auto-added
                    "博敏电子",  # auto-added
                    "超捷股份",  # auto-added
                    "江海股份",  # auto-added
                    "国光电子",  # auto-added
                    "宝鼎科技",  # auto-added
                    "剑桥科技",  # auto-added
                    "光迅科技",  # auto-added
                    "宏景科技",  # auto-added
                    "新凤鸣",  # auto-added
                    "桐昆股份",  # auto-added
                    "恒逸石化",  # auto-added
                    "行云科技",  # auto-added
                    "同大股份",  # auto-added
                    "炬光科技",  # auto-added
                    "昀冢科技",  # auto-added
                    "力源信息",  # auto-added
                    "商络电子",  # auto-added
                    "电科蓝天",  # auto-added
                    "烽火通信",  # auto-added
                    "信科移动",  # auto-added
                    "航天工程",  # auto-added
                    "智谱",  # auto-added
                    "凌玮科技",  # auto-added
                    "新国都",  # auto-added
                    "云天励飞",  # auto-added
                    "海博思创",  # auto-added
                    "新乡化纤",  # auto-added
                    "晋拓股份",  # auto-added
                    "长信科技",  # auto-added
                    "民爆光电",  # auto-added
                    "国巨",  # auto-added
                    "立中集团",  # auto-added
                    "UMTC",  # auto-added
                    "斯菱股份",  # auto-added
                    "正强股份",  # auto-added
                    "铁流股份",  # auto-added
                    "福赛科技",  # auto-added
                    "新泉股份"],
            },
            "设备": {
                "label": "锂电设备",
                "stocks": ["先导智能", "杭可科技", "利元亨",  # auto-added
                    "奥特维",  # auto-added
                    "海目星"],
            },
        },
    },

    "半导体": {
        "icon": "memory",
        "color": "#6366f1",
        "tiers": {
            "上上游": {
                "label": "材料（硅片/靶材/光刻胶/湿电子化学品/特气）",
                "stocks": ["沪硅产业", "立昂微", "江丰电子", "南大光电", "晶瑞电材", "雅克科技", "江化微"],
            },
            "上游": {
                "label": "设备（刻蚀/CVD/测试/单晶炉）",
                "stocks": ["北方华创", "中微公司", "长川科技", "华峰测控", "晶盛机电"],
            },
            "中游设计": {
                "label": "芯片设计",
                "stocks": [
                    "兆易创新", "圣邦股份", "卓胜微", "汇顶科技",  # 韦尔股份待补录
                    "北京君正", "斯达半导", "扬杰科技", "士兰微", "华润微",
                    "瑞芯微", "晶晨股份", "乐鑫科技", "寒武纪", "景嘉微", "紫光国微",  # auto-added
                    "新洁能",  # auto-added
                    "宏微科技",  # auto-added
                    "时代电气",  # auto-added
                    "思瑞浦",  # auto-added
                    "纳芯微",  # auto-added
                    "杰华特",  # auto-added
                    "普冉股份",  # auto-added
                    "东芯股份",  # auto-added
                    "裕太微",  # auto-added
                    "德明利",  # auto-added
                    "大普微",  # auto-added
                    "佰维存储",  # auto-added
                    "芯原股份",  # auto-added
                    "灿芯股份",  # auto-added
                    "翱捷科技",  # auto-added
                    "复旦微电",  # auto-added
                    "国芯科技",  # auto-added
                    "华峰"],
            },
            "中游制造": {
                "label": "晶圆代工 & 封测",
                "stocks": ["中芯国际", "通富微电", "华天科技", "长电科技",  # auto-added
                    "华虹公司",  # auto-added
                    "戈碧迦",  # auto-added
                    "京东方",  # auto-added
                    "深天马",  # auto-added
                    "TCL科技",  # auto-added
                    "芯联集成",  # auto-added
                    "美光科技",  # auto-added
                    "晶合集成",  # auto-added
                    "汇成股份",  # auto-added
                    "新恒汇",  # auto-added
                    "富信",  # auto-added
                    "东方通信",  # auto-added
                    "长鑫科技",  # auto-added
                    "长江存储",  # auto-added
                    "信测标准",  # auto-added
                    "华虹半导体"],
            },
            "下游": {
                "label": "PCB & 被动元件",
                "stocks": ["沪电股份", "景旺电子", "深南电路", "鹏鼎控股", "生益科技", "顺络电子", "法拉电子"],
            },
        },
    },

    "AI算力基础设施": {
        "icon": "hub",
        "color": "#8b5cf6",
        "tiers": {
            "上游": {
                "label": "AI芯片 & GPU",
                "stocks": ["寒武纪", "景嘉微"],
            },
            "中游": {
                "label": "服务器/信创/数据中心配套（供电/散热/光通信）",
                "stocks": [
                    "中科曙光", "润泽科技",          # 数据中心/服务器
                    "泰豪科技",                       # 供电/UPS
                    "科创新源", "申菱环境",            # 液冷散热
                    "天孚通信",                       # 光通信器件
                ],
            },
            "下游": {
                "label": "AI应用 & 云服务",
                "stocks": ["科大讯飞", "中科创达", "虹软科技"],
            },
        },
    },

    "光伏": {
        "icon": "wb_sunny",
        "color": "#f59e0b",
        "tiers": {
            "上游": {
                "label": "硅料 & 辅材",
                "stocks": ["通威股份", "福斯特", "帝科股份", "中信博"],
            },
            "中游": {
                "label": "硅片 & 电池片",
                "stocks": ["隆基绿能", "TCL中环", "晶澳科技", "天合光能"],  # 上机数控待补录
            },
            "下游": {
                "label": "组件 & 逆变器",
                "stocks": ["晶科能源", "阳光电源", "锦浪科技", "固德威", "德业股份"],
            },
            "设备": {
                "label": "光伏设备",
                "stocks": ["迈为股份", "捷佳伟创", "金辰股份"],
            },
        },
    },

    "风电": {
        "icon": "wind_power",
        "color": "#06b6d4",
        "tiers": {
            "上游": {
                "label": "铸件/主轴/轴承/叶片/碳纤维/玻纤",
                "stocks": ["日月股份", "金雷股份", "新强联", "中材科技", "中国巨石", "光威复材"],
            },
            "中游": {
                "label": "整机制造",
                "stocks": ["金风科技", "明阳智能", "运达股份"],
            },
            "下游": {
                "label": "塔筒/海缆/升压站",
                "stocks": ["天顺风能", "大金重工", "东方电缆", "长缆科技"],
            },
        },
    },

    "储能": {
        "icon": "battery_charging_full",
        "color": "#3b82f6",
        "tiers": {
            "上游": {
                "label": "储能电池",
                "stocks": ["宁德时代", "派能科技", "亿纬锂能", "鹏辉能源"],
            },
            "中游": {
                "label": "PCS逆变器/变流器/电能质量",
                "stocks": ["阳光电源", "盛弘股份", "正泰电器"],
            },
            "下游": {
                "label": "系统集成 & 运营",
                "stocks": ["国电南瑞", "林洋能源"],
            },
        },
    },

    "电力设备": {
        "icon": "electrical_services",
        "color": "#f97316",
        "tiers": {
            "上游": {
                "label": "发电设备/变压器/高压开关",
                "stocks": ["东方电气", "特变电工", "平高电气", "思源电气", "大连电瓷", "伊戈尔"],
            },
            "中游": {
                "label": "自动化/继电保护/电网信息化",
                "stocks": ["国电南瑞", "许继电气", "国电南自", "金智科技"],
            },
            "配套": {
                "label": "电缆/电力机器人/电力工程",
                "stocks": ["东方电缆", "长缆科技", "亿嘉和", "中国电建", "永福股份",  # auto-added
                    "远东股份"],
            },
            "终端": {
                "label": "低压电器/智能电表/充电桩",
                "stocks": ["正泰电器", "良信股份", "林洋能源", "特锐德",  # auto-added
                    "宏发股份"],
            },
        },
    },

    "军工": {
        "icon": "security",
        "color": "#ef4444",
        "tiers": {
            "上游": {
                "label": "材料（钛合金/碳纤维/特钢/高温合金）",
                "stocks": ["西部超导", "宝钛股份", "光威复材", "中简科技", "抚顺特钢", "应流股份"],
            },
            "中游": {
                "label": "元器件（连接器/MLCC/雷达/红外）",
                "stocks": ["中航光电", "航天电器", "振华科技", "火炬电子", "鸿远电子", "高德红外"],
            },
            "下游": {
                "label": "整机/平台（飞机/舰船/卫星/超材料）",
                "stocks": ["航发动力", "航发控制", "中航沈飞", "中航西飞", "洪都航空", "中国卫星", "中国船舶", "光启技术"],
            },
        },
    },

    "消费电子": {
        "icon": "smartphone",
        "color": "#ec4899",
        "tiers": {
            "上游": {
                "label": "PCB/FPC/连接器/覆铜板/被动元件",
                "stocks": ["沪电股份", "景旺电子", "鹏鼎控股", "生益科技", "弘信电子", "意华股份", "ST得润"],
            },
            "中游": {
                "label": "声学/光学/结构件/精密制造",
                "stocks": ["立讯精密", "歌尔股份", "东山精密", "欧菲光", "水晶光电", "领益智造"],
            },
            "下游": {
                "label": "结构件/光学/精密制造/ODM/显控",
                "stocks": ["蓝思科技", "闻泰科技", "环旭电子", "视源股份", "深天马Ａ", "京东方Ａ"],
            },
        },
    },

    "智能驾驶": {
        "icon": "directions_car",
        "color": "#14b8a6",
        "tiers": {
            "感知层": {
                "label": "摄像头/雷达/传感器",
                "stocks": ["富瀚微"],  # 韦尔股份待补录
            },
            "计算层": {
                "label": "域控制器/算法/座舱",
                "stocks": ["德赛西威", "虹软科技", "中科创达"],
            },
            "执行层": {
                "label": "线控底盘/电机电控/零部件",
                "stocks": ["三花智控", "拓普集团", "均胜电子", "汇川技术"],
            },
        },
    },

    "能源电力运营": {
        "icon": "bolt",
        "color": "#eab308",
        "tiers": {
            "水电运营": {
                "label": "水力发电",
                "stocks": ["长江电力", "华能水电", "川投能源", "黔源电力"],
            },
            "火电运营": {
                "label": "火力发电",
                "stocks": ["华能国际", "华电国际", "内蒙华电", "华银电力"],
            },
            "新能源运营": {
                "label": "新能源发电运营",
                "stocks": ["三峡能源", "嘉泽新能", "节能风电", "太阳能"],
            },
            "天然气/综合": {
                "label": "天然气分销/城市燃气",
                "stocks": ["新天然气", "广汇能源", "蓝焰控股"],
            },
        },
    },

    "化工新材料": {
        "icon": "science",
        "color": "#84cc16",
        "tiers": {
            "上游": {
                "label": "基础化工（氟化工/有机硅/MDI）",
                "stocks": ["万华化学", "巨化股份", "三美股份", "合盛硅业", "新安股份", "卫星化学"],  # 卫星化学=原卫星石化，已在DB
            },
            "中游": {
                "label": "特种气体/精细化工",
                "stocks": ["金宏气体", "凯美特气", "雅克科技", "昊华科技"],
            },
            "下游": {
                "label": "改性塑料/功能材料/染料",
                "stocks": ["金发科技", "泰和新材", "浙江龙盛", "华峰化学"],
            },
            "下游-染料/精细化工": {
                "label": "染料/精细化工",
                "stocks": ["浙江龙盛", "亚邦股份",  # auto-added
                    "亚香股份"],
            },
        },
    },

    "钢铁基建材料": {
        "icon": "foundation",
        "color": "#78716c",
        "tiers": {
            "上游-矿产": {
                "label": "铁矿石/焦煤",
                "stocks": ["海南矿业"],
            },
            "中游-特钢": {
                "label": "特种钢材/高温合金",
                "stocks": ["永兴材料", "抚顺特钢", "南钢股份", "方大特钢", "广大特材",  # auto-added
                    "天工国际"],
            },
            "中游-普钢": {
                "label": "普通钢铁",
                "stocks": ["宝钢股份", "太钢不锈"],
            },
            "中游-建材": {
                "label": "水泥/玻纤/涂料",
                "stocks": ["海螺水泥", "上峰水泥", "中国巨石", "三棵树", "福莱特",  # auto-added
                    "国际复材"],  # 华新水泥待补录
            },
        },
    },

    "碳中和环保": {
        "icon": "eco",
        "color": "#22c55e",
        "tiers": {
            "资源回收": {
                "label": "废旧电池回收/金属再生",
                "stocks": ["格林美", "华宏科技", "天奇股份"],
            },
            "危废处理": {
                "label": "危废/工业固废处理",
                "stocks": ["浙富控股"],
            },
            "节能减排": {
                "label": "环保工程/碳捕集",
                "stocks": [],  # 远达环保待补录
            },
        },
    },

    "医药CRO": {
        "icon": "biotech",
        "color": "#f43f5e",
        "tiers": {
            "临床前CRO": {
                "label": "临床前研究外包",
                "stocks": ["昭衍新药", "药石科技"],
            },
            "临床CRO": {
                "label": "临床研究外包",
                "stocks": ["药明康德", "泰格医药", "康龙化成", "美迪西", "博济医药"],
            },
            "CDMO": {
                "label": "合同研发生产",
                "stocks": ["凯莱英", "博腾股份", "九洲药业",  # auto-added
                    "神州细胞"],
            },
        },
    },

    "稀土永磁": {
        "icon": "magnet",
        "color": "#a855f7",
        "tiers": {
            "上游": {
                "label": "稀土矿采选/分离",
                "stocks": ["北方稀土", "盛和资源"],  # 广晟有色、五矿稀土待补录
            },
            "中游": {
                "label": "钕铁硼永磁材料",
                "stocks": ["中科三环", "正海磁材", "金力永磁", "大地熊", "宁波韵升"],
            },
            "下游": {
                "label": "新能源车电机/工业电机/风电",
                "stocks": ["汇川技术", "卧龙电驱", "金风科技"],
            },
        },
    },

    "氢能": {
        "icon": "grain",
        "color": "#10b981",
        "tiers": {
            "中游": {
                "label": "绿氢制备与供应",
                "stocks": ["复洁科技"],  # auto-added
            },
        },
    },

    "工程机械": {
        "icon": "precision_manufacturing",
        "color": "#f59e0b",
        "tiers": {
            "整机": {
                "label": "工程机械整机制造",
                "stocks": ["宏观",  # auto-added
                    "三一重工"],  # auto-added
            },
        },
    },

    "商用车": {
        "icon": "local_shipping",
        "color": "#f59e0b",
        "tiers": {
            "整机": {
                "label": "商用车整机制造",
                "stocks": ["中国重汽H"],  # auto-added
            },
        },
    },

    "消费品牌": {
        "icon": "category",
        "color": "#d946ef",
        "tiers": {
            "多元业务": {
                "label": "电池品牌+硅光芯片",
                "stocks": ["安孚科技"],  # auto-added
            },
            "休闲食品": {
                "label": "休闲食品品牌",
                "stocks": ["洽洽食品"],  # auto-added
            },
        },
    },

    "金融服务": {
        "icon": "category",
        "color": "#6366f1",
        "tiers": {
            "券商": {
                "label": "综合券商",
                "stocks": ["华泰证券"],  # auto-added
            },
        },
    },

    "医疗器械": {
        "icon": "biotech",
        "color": "#10b981",
        "tiers": {
            "消费医疗": {
                "label": "消费医疗/医美器械",
                "stocks": ["乐普医疗"],  # auto-added
            },
        },
    },

    "战略金属": {
        "icon": "grain",
        "color": "#ef4444",
        "tiers": {
            "资源采选": {
                "label": "钨矿采选冶炼",
                "stocks": ["中钨高新"],  # auto-added
            },
        },
    },

    "在线旅游": {
        "icon": "flight",
        "color": "#0ea5e9",
        "tiers": {
            "OTA平台": {
                "label": "OTA综合平台",
                "stocks": ["携程集团-S"],  # auto-added
            },
        },
    },

    "油气设备": {
        "icon": "precision_manufacturing",
        "color": "#f59e0b",
        "tiers": {
            "设备制造": {
                "label": "深海油气装备制造",
                "stocks": ["博迈科"],  # auto-added
            },
        },
    },

    "跨境电商": {
        "icon": "local_shipping",
        "color": "#10b981",
        "tiers": {
            "平台运营": {
                "label": "跨境电商运营",
                "stocks": ["吉宏股份"],  # auto-added
            },
        },
    },

    "金刚石散热": {
        "icon": "precision_manufacturing",
        "color": "#d946ef",
        "tiers": {
            "材料制备": {
                "label": "金刚石材料制备",
                "stocks": ["国机精工"],  # auto-added
            },
        },
    },
}


# 股票标签：leader=产业链龙头股  news=Daily Intel 新闻推荐自动写入
STOCK_TAGS = {
    "赣锋锂业": "leader",
    "天齐锂业": "leader",
    "华友钴业": "leader",
    "洛阳钼业": "leader",
    "盛新锂能": "leader",
    "盐湖股份": "leader",
    "容百科技": "leader",
    "当升科技": "leader",
    "德方纳米": "leader",
    "中伟股份": "leader",
    "璞泰来": "leader",
    "杉杉股份": "leader",
    "中科电气": "leader",
    "恩捷股份": "leader",
    "星源材质": "leader",
    "天赐材料": "leader",
    "新宙邦": "leader",
    "诺德股份": "leader",
    "嘉元科技": "leader",
    "宁德时代": "leader",
    "亿纬锂能": "leader",
    "国轩高科": "leader",
    "汇川技术": "leader",
    "卧龙电驱": "leader",
    "大洋电机": "leader",
    "比亚迪": "leader",
    "长安汽车": "leader",
    "长城汽车": "leader",
    "北汽蓝谷": "leader",
    "拓普集团": "leader",
    "均胜电子": "leader",
    "宁波华翔": "leader",
    "先导智能": "leader",
    "杭可科技": "leader",
    "利元亨": "leader",
    "沪硅产业": "leader",
    "立昂微": "leader",
    "江丰电子": "leader",
    "南大光电": "leader",
    "晶瑞股份": "leader",
    "雅克科技": "leader",
    "江化微": "leader",
    "北方华创": "leader",
    "中微公司": "leader",
    "长川科技": "leader",
    "华峰测控": "leader",
    "晶盛机电": "leader",
    "韦尔股份": "leader",
    "兆易创新": "leader",
    "圣邦股份": "leader",
    "卓胜微": "leader",
    "汇顶科技": "leader",
    "北京君正": "leader",
    "斯达半导": "leader",
    "扬杰科技": "leader",
    "士兰微": "leader",
    "华润微": "leader",
    "瑞芯微": "leader",
    "晶晨股份": "leader",
    "乐鑫科技": "leader",
    "寒武纪": "leader",
    "景嘉微": "leader",
    "紫光国微": "leader",
    "中芯国际": "leader",
    "通富微电": "leader",
    "华天科技": "leader",
    "长电科技": "leader",
    "沪电股份": "leader",
    "景旺电子": "leader",
    "深南电路": "leader",
    "鹏鼎控股": "leader",
    "生益科技": "leader",
    "顺络电子": "leader",
    "法拉电子": "leader",
    "寒武纪": "leader",
    "景嘉微": "leader",
    "中科曙光": "leader",
    "润泽科技": "leader",
    "泰豪科技": "leader",
    "科创新源": "leader",
    "申菱环境": "leader",
    "天孚通信": "leader",
    "科大讯飞": "leader",
    "中科创达": "leader",
    "虹软科技": "leader",
    "通威股份": "leader",
    "福斯特": "leader",
    "帝科股份": "leader",
    "中信博": "leader",
    "隆基绿能": "leader",
    "中环股份": "leader",
    "上机数控": "leader",
    "晶澳科技": "leader",
    "天合光能": "leader",
    "晶科能源": "leader",
    "阳光电源": "leader",
    "锦浪科技": "leader",
    "固德威": "leader",
    "德业股份": "leader",
    "迈为股份": "leader",
    "捷佳伟创": "leader",
    "金辰股份": "leader",
    "日月股份": "leader",
    "金雷股份": "leader",
    "新强联": "leader",
    "中材科技": "leader",
    "中国巨石": "leader",
    "光威复材": "leader",
    "金风科技": "leader",
    "明阳智能": "leader",
    "运达股份": "leader",
    "天顺风能": "leader",
    "大金重工": "leader",
    "东方电缆": "leader",
    "长缆科技": "leader",
    "宁德时代": "leader",
    "派能科技": "leader",
    "亿纬锂能": "leader",
    "鹏辉能源": "leader",
    "阳光电源": "leader",
    "盛弘股份": "leader",
    "正泰电器": "leader",
    "国电南瑞": "leader",
    "林洋能源": "leader",
    "东方电气": "leader",
    "特变电工": "leader",
    "平高电气": "leader",
    "思源电气": "leader",
    "大连电瓷": "leader",
    "伊戈尔": "leader",
    "国电南瑞": "leader",
    "许继电气": "leader",
    "国电南自": "leader",
    "金智科技": "leader",
    "东方电缆": "leader",
    "长缆科技": "leader",
    "亿嘉和": "leader",
    "中国电建": "leader",
    "永福股份": "leader",
    "正泰电器": "leader",
    "良信股份": "leader",
    "林洋能源": "leader",
    "特锐德": "leader",
    "西部超导": "leader",
    "宝钛股份": "leader",
    "光威复材": "leader",
    "中简科技": "leader",
    "抚顺特钢": "leader",
    "应流股份": "leader",
    "中航光电": "leader",
    "航天电器": "leader",
    "振华科技": "leader",
    "火炬电子": "leader",
    "鸿远电子": "leader",
    "高德红外": "leader",
    "航发动力": "leader",
    "航发控制": "leader",
    "中航沈飞": "leader",
    "中航西飞": "leader",
    "洪都航空": "leader",
    "中国卫星": "leader",
    "中国船舶": "leader",
    "光启技术": "leader",
    "沪电股份": "leader",
    "景旺电子": "leader",
    "鹏鼎控股": "leader",
    "生益科技": "leader",
    "弘信电子": "leader",
    "意华股份": "leader",
    "得润电子": "leader",
    "立讯精密": "leader",
    "歌尔股份": "leader",
    "东山精密": "leader",
    "欧菲光": "leader",
    "水晶光电": "leader",
    "领益智造": "leader",
    "蓝思科技": "leader",
    "闻泰科技": "leader",
    "环旭电子": "leader",
    "视源股份": "leader",
    "深天马Ａ": "leader",
    "京东方Ａ": "leader",
    "韦尔股份": "leader",
    "富瀚微": "leader",
    "德赛西威": "leader",
    "虹软科技": "leader",
    "中科创达": "leader",
    "三花智控": "leader",
    "拓普集团": "leader",
    "均胜电子": "leader",
    "汇川技术": "leader",
    "长江电力": "leader",
    "华能水电": "leader",
    "川投能源": "leader",
    "黔源电力": "leader",
    "华能国际": "leader",
    "华电国际": "leader",
    "内蒙华电": "leader",
    "华银电力": "leader",
    "三峡能源": "leader",
    "嘉泽新能": "leader",
    "节能风电": "leader",
    "太阳能": "leader",
    "新天然气": "leader",
    "广汇能源": "leader",
    "蓝焰控股": "leader",
    "万华化学": "leader",
    "巨化股份": "leader",
    "三美股份": "leader",
    "合盛硅业": "leader",
    "新安股份": "leader",
    "卫星石化": "leader",
    "金宏气体": "leader",
    "凯美特气": "leader",
    "雅克科技": "leader",
    "昊华科技": "leader",
    "金发科技": "leader",
    "泰和新材": "leader",
    "浙江龙盛": "leader",
    "华峰化学": "leader",
    "浙江龙盛": "leader",
    "亚邦股份": "leader",
    "海南矿业": "leader",
    "永兴材料": "leader",
    "抚顺特钢": "leader",
    "南钢股份": "leader",
    "方大特钢": "leader",
    "广大特材": "leader",
    "宝钢股份": "leader",
    "太钢不锈": "leader",
    "海螺水泥": "leader",
    "华新水泥": "leader",
    "上峰水泥": "leader",
    "中国巨石": "leader",
    "三棵树": "leader",
    "福莱特": "leader",
    "格林美": "leader",
    "华宏科技": "leader",
    "天奇股份": "leader",
    "浙富控股": "leader",
    "远达环保": "leader",
    "昭衍新药": "leader",
    "药石科技": "leader",
    "药明康德": "leader",
    "泰格医药": "leader",
    "康龙化成": "leader",
    "美迪西": "leader",
    "博济医药": "leader",
    "药明生物": "leader",
    "凯莱英": "leader",
    "博腾股份": "leader",
    "九洲药业": "leader",
    "北方稀土": "leader",
    "盛和资源": "leader",
    "广晟有色": "leader",
    "五矿稀土": "leader",
    "中科三环": "leader",
    "正海磁材": "leader",
    "金力永磁": "leader",
    "大地熊": "leader",
    "宁波韵升": "leader",
    "汇川技术": "leader",
    "卧龙电驱": "leader",
    "金风科技": "leader",

    "胜宏科技": "news",  # auto-added

    "世运电路": "news",  # auto-added

    "奥士康": "news",  # auto-added

    "华虹公司": "news",  # auto-added

    "朝阳科技": "news",  # auto-added

    "极氪": "news",  # auto-added

    "吉利汽车": "news",  # auto-added

    "小鹏汽车": "news",  # auto-added

    "亚香股份": "news",  # auto-added

    "复洁科技": "news",  # auto-added

    "宏观": "news",  # auto-added

    "奥特维": "news",  # auto-added

    "东方日升": "news",  # auto-added

    "东威科技": "news",  # auto-added

    "鼎泰高科": "news",  # auto-added

    "京东方A": "news",  # auto-added

    "帝尔激光": "news",  # auto-added

    "大族激光": "news",  # auto-added

    "星宸科技": "news",  # auto-added

    "海目星": "news",  # auto-added

    "中际旭创": "news",  # auto-added

    "新易盛": "news",  # auto-added

    "工业富联": "news",  # auto-added

    "银轮股份": "news",  # auto-added

    "天润工业": "news",  # auto-added

    "中富电路": "news",  # auto-added

    "拓荆科技": "news",  # auto-added

    "华海清科": "news",  # auto-added

    "盛美上海": "news",  # auto-added

    "鸿仕达": "news",  # auto-added

    "晶升股份": "news",  # auto-added

    "新益昌": "news",  # auto-added

    "联动科技": "news",  # auto-added

    "精智达": "news",  # auto-added

    "金海通": "news",  # auto-added

    "强一股份": "news",  # auto-added

    "金富科技": "news",  # auto-added

    "五洋自控": "news",  # auto-added

    "英维克": "news",  # auto-added

    "思泉新材": "news",  # auto-added

    "亚威股份": "news",  # auto-added

    "洁美科技": "news",  # auto-added

    "博迁新材": "news",  # auto-added

    "国瓷材料": "news",  # auto-added

    "三环集团": "news",  # auto-added

    "风华高科": "news",  # auto-added

    "艾华集团": "news",  # auto-added

    "强瑞技术": "news",  # auto-added

    "东阳光": "news",  # auto-added

    "秦淮数据": "news",  # auto-added

    "芯碁微装": "news",  # auto-added

    "华盛锂电": "news",  # auto-added

    "海科新源": "news",  # auto-added

    "富祥药业": "news",  # auto-added

    "孚日股份": "news",  # auto-added

    "海辰药业": "news",  # auto-added

    "ASML": "news",  # auto-added

    "科翔股份": "news",  # auto-added

    "博敏电子": "news",  # auto-added

    "英杰电气": "news",  # auto-added

    "恒运昌": "news",  # auto-added

    "超捷股份": "news",  # auto-added

    "沐曦股份": "news",  # auto-added

    "天数智芯": "news",  # auto-added

    "南亚新材": "news",  # auto-added

    "江海股份": "news",  # auto-added

    "国光电子": "news",  # auto-added

    "国城矿业": "news",  # auto-added

    "宝鼎科技": "news",  # auto-added

    "智立方": "news",  # auto-added

    "盛剑科技": "news",  # auto-added

    "大中矿业": "news",  # auto-added

    "高测股份": "news",  # auto-added

    "建滔积层板": "news",  # auto-added

    "国际复材": "news",  # auto-added

    "德福科技": "news",  # auto-added

    "铜冠铜箔": "news",  # auto-added

    "海亮股份": "news",  # auto-added

    "洪田股份": "news",  # auto-added

    "泰金新能": "news",  # auto-added

    "新洁能": "news",  # auto-added

    "宏发股份": "news",  # auto-added

    "潍柴动力": "news",  # auto-added

    "杰瑞股份": "news",  # auto-added

    "中国动力": "news",  # auto-added

    "豪迈科技": "news",  # auto-added

    "万泽股份": "news",  # auto-added

    "汽轮科技": "news",  # auto-added

    "潍柴重机": "news",  # auto-added

    "联德股份": "news",  # auto-added

    "国泰海通": "news",  # auto-added

    "中信证券": "news",  # auto-added

    "广发证券": "news",  # auto-added

    "中金公司": "news",  # auto-added

    "同花顺": "news",  # auto-added

    "仙鹤股份": "news",  # auto-added

    "时代新材": "news",  # auto-added

    "兴发集团": "news",  # auto-added

    "中钨高新": "news",  # auto-added

    "滨化股份": "news",  # auto-added

    "天工国际": "news",  # auto-added

    "海光信息": "news",  # auto-added

    "剑桥科技": "news",  # auto-added

    "戈碧迦": "news",  # auto-added

    "京东方": "news",  # auto-added

    "深天马": "news",  # auto-added

    "TCL科技": "news",  # auto-added

    "天岳先进": "news",  # auto-added

    "三安光电": "news",  # auto-added

    "芯联集成": "news",  # auto-added

    "宏微科技": "news",  # auto-added

    "时代电气": "news",  # auto-added

    "金盘科技": "news",  # auto-added

    "四方股份": "news",  # auto-added

    "兴福电子": "news",  # auto-added

    "云南锗业": "news",  # auto-added

    "光迅科技": "news",  # auto-added

    "长飞光纤": "news",  # auto-added

    "亨通光电": "news",  # auto-added

    "中天科技": "news",  # auto-added

    "沃格光电": "news",  # auto-added

    "凯盛科技": "news",  # auto-added

    "美光科技": "news",  # auto-added

    "永鼎股份": "news",  # auto-added

    "宏景科技": "news",  # auto-added

    "舜宇光学科技": "news",  # auto-added

    "晶合集成": "news",  # auto-added

    "思瑞浦": "news",  # auto-added

    "纳芯微": "news",  # auto-added

    "杰华特": "news",  # auto-added

    "鼎龙股份": "news",  # auto-added

    "飞凯材料": "news",  # auto-added

    "彤程新材": "news",  # auto-added

    "茂莱光学": "news",  # auto-added

    "横店东磁": "news",  # auto-added

    "新凤鸣": "news",  # auto-added

    "桐昆股份": "news",  # auto-added

    "恒逸石化": "news",  # auto-added

    "汇成股份": "news",  # auto-added

    "康宁": "news",  # auto-added

    "臻宝科技": "news",  # auto-added

    "海星股份": "news",  # auto-added

    "王子新材": "news",  # auto-added

    "亚翔集成": "news",  # auto-added

    "万裕科技": "news",  # auto-added

    "立隆电": "news",  # auto-added

    "川润股份": "news",  # auto-added

    "同星科技": "news",  # auto-added

    "胜蓝股份": "news",  # auto-added

    "美利信": "news",  # auto-added

    "中鼎股份": "news",  # auto-added

    "溯联股份": "news",  # auto-added

    "川环科技": "news",  # auto-added

    "鸿富瀚": "news",  # auto-added

    "锦富技术": "news",  # auto-added

    "大元泵业": "news",  # auto-added

    "飞龙股份": "news",  # auto-added

    "行云科技": "news",  # auto-added

    "厦门钨业": "news",  # auto-added

    "翔鹭钨业": "news",  # auto-added

    "章源钨业": "news",  # auto-added

    "金钼股份": "news",  # auto-added

    "中国稀土": "news",  # auto-added

    "驰宏锌锗": "news",  # auto-added

    "锡业股份": "news",  # auto-added

    "株冶集团": "news",  # auto-added

    "豫光金铅": "news",  # auto-added

    "有研新材": "news",  # auto-added

    "宏达电子": "news",  # auto-added

    "宏明电子": "news",  # auto-added

    "华润饮料": "news",  # auto-added

    "H&H国际控股": "news",  # auto-added

    "同大股份": "news",  # auto-added

    "炬光科技": "news",  # auto-added

    "太辰光": "news",  # auto-added

    "仕佳光子": "news",  # auto-added

    "衡东光": "news",  # auto-added

    "昀冢科技": "news",  # auto-added

    "力源信息": "news",  # auto-added

    "商络电子": "news",  # auto-added

    "神工股份": "news",  # auto-added

    "普冉股份": "news",  # auto-added

    "东芯股份": "news",  # auto-added

    "裕太微": "news",  # auto-added

    "盛科通信": "news",  # auto-added

    "电科蓝天": "news",  # auto-added

    "航天电子": "news",  # auto-added

    "国博电子": "news",  # auto-added

    "陕西华达": "news",  # auto-added

    "西部材料": "news",  # auto-added

    "智明达": "news",  # auto-added

    "中航高科": "news",  # auto-added

    "六九一二": "news",  # auto-added

    "烽火通信": "news",  # auto-added

    "信科移动": "news",  # auto-added

    "信维通信": "news",  # auto-added

    "斯瑞新材": "news",  # auto-added

    "上海瀚讯": "news",  # auto-added

    "航天工程": "news",  # auto-added

    "高华科技": "news",  # auto-added

    "西测测试": "news",  # auto-added

    "铂力特": "news",  # auto-added

    "莲花控股": "news",  # auto-added

    "通鼎互联": "news",  # auto-added

    "杭电股份": "news",  # auto-added

    "携程集团-S": "news",  # auto-added

    "神州细胞": "news",  # auto-added

    "中矿资源": "news",  # auto-added

    "雅化集团": "news",  # auto-added

    "松发股份": "news",  # auto-added

    "建滔集团": "news",  # auto-added

    "华正新材": "news",  # auto-added

    "金安国际": "news",  # auto-added

    "华凯易佰": "news",  # auto-added

    "三一重工": "news",  # auto-added

    "新恒汇": "news",  # auto-added

    "九安医疗": "news",  # auto-added

    "富信": "news",  # auto-added

    "特发信息": "news",  # auto-added

    "东方通信": "news",  # auto-added

    "智谱": "news",  # auto-added

    "长鑫科技": "news",  # auto-added

    "长江存储": "news",  # auto-added

    "德明利": "news",  # auto-added

    "大普微": "news",  # auto-added

    "佰维存储": "news",  # auto-added

    "远东股份": "news",  # auto-added

    "凌玮科技": "news",  # auto-added

    "力诺药包": "news",  # auto-added

    "元力股份": "news",  # auto-added

    "新国都": "news",  # auto-added

    "信测标准": "news",  # auto-added

    "芯原股份": "news",  # auto-added

    "灿芯股份": "news",  # auto-added

    "翱捷科技": "news",  # auto-added

    "复旦微电": "news",  # auto-added

    "云天励飞": "news",  # auto-added

    "国芯科技": "news",  # auto-added

    "宏柏新材": "news",  # auto-added

    "容大感光": "news",  # auto-added

    "八亿时空": "news",  # auto-added

    "圣泉集团": "news",  # auto-added

    "东材科技": "news",  # auto-added

    "久日新材": "news",  # auto-added

    "强力新材": "news",  # auto-added

    "万润股份": "news",  # auto-added

    "瑞联新材": "news",  # auto-added

    "华虹半导体": "news",  # auto-added

    "海博思创": "news",  # auto-added

    "新乡化纤": "news",  # auto-added

    "斯莱克": "news",  # auto-added

    "国机精工": "news",  # auto-added

    "四方达": "news",  # auto-added

    "中兵红箭": "news",  # auto-added

    "黄河旋风": "news",  # auto-added

    "力量钻石": "news",  # auto-added

    "沃尔德": "news",  # auto-added

    "华峰": "news",  # auto-added

    "国泰君安": "news",  # auto-added

    "海通证券": "news",  # auto-added

    "招商证券": "news",  # auto-added

    "天承科技": "news",  # auto-added

    "宸展光电": "news",  # auto-added

    "汇成真空": "news",  # auto-added

    "中国重汽H": "news",  # auto-added

    "安孚科技": "news",  # auto-added

    "华泰证券": "news",  # auto-added

    "乐普医疗": "news",  # auto-added

    "洽洽食品": "news",  # auto-added

    "博迈科": "news",  # auto-added

    "吉宏股份": "news",  # auto-added

    "中导光电": "news",  # auto-added

    "晋拓股份": "news",  # auto-added

    "东方国信": "news",  # auto-added

    "大位科技": "news",  # auto-added

    "半导体设备板块": "news",  # auto-added

    "长信科技": "news",  # auto-added

    "微导纳米": "news",  # auto-added

    "宝明科技": "news",  # auto-added

    "方源新能源": "news",  # auto-added

    "台新股份": "news",  # auto-added

    "民爆光电": "news",  # auto-added

    "国巨": "news",  # auto-added

    "正帆科技": "news",  # auto-added

    "先锋精科": "news",  # auto-added

    "富创精密": "news",  # auto-added

    "新莱应材": "news",  # auto-added

    "隆达股份": "news",  # auto-added

    "石大胜华": "news",  # auto-added

    "永和股份": "news",  # auto-added

    "楚江新材": "news",  # auto-added

    "立中集团": "news",  # auto-added

    "奥飞数据": "news",  # auto-added

    "万国数据": "news",  # auto-added

    "世纪互联": "news",  # auto-added

    "英伟达": "news",  # auto-added

    "AMD": "news",  # auto-added

    "UMTC": "news",  # auto-added

    "斯菱股份": "news",  # auto-added

    "正强股份": "news",  # auto-added

    "铁流股份": "news",  # auto-added

    "福赛科技": "news",  # auto-added

    "新泉股份": "news",  # auto-added

    "汉钟精机": "news",  # auto-added

    "中科仪": "news",  # auto-added

    "华盛昌": "news",  # auto-added

    "联讯仪器": "news",  # auto-added

    "精测电子": "news",  # auto-added

    "天华新能": "news",  # auto-added

    "东岳集团": "news",  # auto-added
}
