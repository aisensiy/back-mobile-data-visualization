#!/usr/bin/env python
# encoding: utf-8

from collections import Counter


tag_whitelist = set(map(lambda x: x.split(':')[0], open('top100.tags.txt').readlines()))

tag_blacklist = set(['地名地址信息', '交通地名', '村庄级地名',
                     '路口名', '热点地名', '桥', '区县级地名',
                     '普通地名', '出入口', '楼栋号',
                     '高速路入口', '楼宇', '门牌信息'])

tag_convertlist = {
    '中餐厅': '餐饮服务',
    '快餐厅': '餐饮服务',
    '糕饼店': '餐饮服务',
    '清真菜馆': '餐饮服务',
    '餐饮相关': '餐饮服务',
    '茶艺馆': '娱乐场所',
    '海鲜酒楼': '餐饮服务',
    '火锅店': '餐饮服务',
    '四川菜(川菜)': '餐饮服务',
    '公司': '公司企业',
    '住宿服务': '宾馆酒店',
    '四星级宾馆': '宾馆酒店',
    '五星级宾馆': '宾馆酒店',
    '三星级宾馆': '宾馆酒店',
    '旅馆招待所': '宾馆酒店',
    '科教文化场所': '科教文化服务',
    '特色/地方风味餐厅': '餐饮服务',
    '经济型连锁酒店': '餐饮服务',
    '区县级政府及事业单位': '政府机构及社会团体',
    '政府机关': '政府机构及社会团体',
    '省直辖市级政府及事业单位': '政府机构及社会团体',
    '社会团体': '社会团体',
    '金融保险机构': '金融保险服务机构',
    '超级市场': '超市',
    '综合医院': '医院',
    '专科医院': '医院',
    '卫生院': '医院',
    '住宅小区': '住宅区',
    '公园': '公园广场'
}


def raw2dict(raw):
    if raw:
        tag_dict = dict(map(lambda x: (x.split(':')[0],
                                       int(x.split(':')[1])),
                            raw.split(' ')))
    else:
        tag_dict = {'notag': 1}

    return tag_dict


def _clean_tags(tag_dict):
    for tag in tag_blacklist:
        if tag not in tag_dict:
            continue
        del tag_dict[tag]

    for k, v in tag_convertlist.items():
        if k not in tag_dict:
            continue

        if v in tag_dict:
            tag_dict[v] = tag_dict[v] + tag_dict[k]
        else:
            tag_dict[v] = tag_dict[k]

        del tag_dict[k]

    for tag in tag_dict.keys():
        if tag not in tag_whitelist:
            del tag_dict[tag]

    if not len(tag_dict):
        tag_dict['notag'] = 1


def clean_tags(raw, n=5):
    tag_dict = raw2dict(raw)
    tag_dict = dict(Counter(tag_dict).most_common()[:n])
    _clean_tags(tag_dict)
    return tag_dict
