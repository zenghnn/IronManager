package IronManager

type UserM struct {
	Id           int64  `gorm:"column:id;size:19;type:bigint;" json:"id"`
	OpenId       string `gorm:"column:open_id;size:100;type:varchar(100);" json:"open_id"`
	Mobile       string `gorm:"column:mobile;size:100;type:varchar(100);" json:"mobile"`
	Account      string `gorm:"column:account;size:100;type:varchar(100);" json:"account"`
	Pwd          string `gorm:"column:password;size:100;type:varchar(100);" json:"password"`
	AccType      int    `gorm:"column:acc_type;type:int;" json:"acc_type"`
	HeadUrl      string `gorm:"column:head_url;size:500;type:varchar(500);" json:"head_url"`
	Country      string `gorm:"column:country;size:100;type:varchar(100);" json:"country"`
	CreateIp     string `gorm:"column:create_ip;size:15;type:varchar(15);" json:"create_ip"`
	CreateIpV6   string `gorm:"column:create_ip_v6;size:255;type:varchar(255);" json:"create_ip_v6"`
	NickName     string `gorm:"column:nick_name;size:200;type:varchar(200);" json:"nick_name"`
	Channel      int    `gorm:"column:channel;size:10;type:int;" json:"channel"`
	Platform     string `gorm:"column:platform;size:255;type:varchar(255);" json:"platform"`
	Forbidden    int    `gorm:"column:forbidden;size:10;type:int;" json:"forbidden"`          // 默认0正常  1-帐号被禁 2-时限禁用
	ForbiddenEnd string `gorm:"column:forbidden_end;type:timestamp(3);" json:"forbidden_end"` // 如果forb为2到时间结束禁用结束
	Role         int    `gorm:"column:role;size:10;type:int;" json:"role"`                    // 角色0-玩家  1-测试人员
}

type RegularUse struct {
	Uid            int64                  `gorm:"column:uid;size:19;type:bigint;" json:"uid"`                                // 对应用户主表中id
	Money          int64                  `gorm:"column:money;size:19;type:bigint;" json:"money"`                            // 当前金币
	MoneyGive      int64                  `gorm:"column:money_give;size:19;type:bigint;" json:"money_give"`                  // 系统给的金币
	MoneyCharge    int64                  `gorm:"column:money_charge;size:19;type:bigint;" json:"money_charge"`              // 用户充值金币
	ShopScheme     map[string]interface{} `gorm:"column:shop_scheme;type:json;" json:"shop_scheme"`                          // 当前店铺的顾客流
	RoomScheme     map[string]interface{} `gorm:"column:room_scheme;type:json;" json:"room_scheme"`                          // 当前卧室的活动
	CatCollections string                 `gorm:"column:cat_collections;size:255;type:varchar(255);" json:"cat_collections"` // 猫收集的物品
	ShopGeomancy   string                 `gorm:"column:shop_geomancy;size:255;type:varchar(255);" json:"shop_geomancy"`     // 商店风水(即装饰特)
	Interesting    int                    `gorm:"column:interesting;size:10;type:int;" json:"interesting"`                   // 商店的魅力点(猫收集品,商店风水物品)
	GoodsOwned     string                 `gorm:"column:goods_owned;size:255;type:varchar(255);" json:"goods_owned"`         // 拥有的商品(售卖)
}
